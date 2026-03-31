"""Filtering and resource management functions for collection tasks."""

import json
import logging
from typing import Dict, List, Optional, Tuple

from digital_land import __version__ as dl_version
from digital_land.utils.hash_utils import hash_directory
from digital_land.utils.dataset_resource_utils import resource_needs_processing

logger = logging.getLogger(__name__)


def build_redirect_map(old_resource_entries: List[Dict]) -> Dict[str, str]:
    """Build a redirect map from old_resource entries.

    Args:
        old_resource_entries: List of old_resource entries from collection

    Returns:
        Dictionary mapping old-resource to resource
    """
    redirect = {}
    for entry in old_resource_entries:
        redirect[entry["old-resource"]] = entry["resource"]
    return redirect


def build_dataset_resource_pairs(
    dataset_resource_map: Dict[str, List[str]],
    dataset: Optional[str] = None
) -> List[Tuple[str, str]]:
    """Build sorted list of (dataset, resource) pairs.

    This preserves duplicates where a resource is used in multiple datasets.

    Args:
        dataset_resource_map: Dictionary mapping datasets to lists of resources
        dataset: Optional dataset name to filter to

    Returns:
        List of (dataset, resource) tuples
    """
    datasets_to_process = [dataset] if dataset else sorted(dataset_resource_map.keys())

    dataset_resource_pairs = []
    for ds in sorted(datasets_to_process):
        if ds not in dataset_resource_map:
            continue
        for resource in sorted(dataset_resource_map[ds]):
            dataset_resource_pairs.append((ds, resource))

    return dataset_resource_pairs


def apply_offset_and_limit(
    dataset_resource_pairs: List[Tuple[str, str]],
    offset: Optional[int] = None,
    limit: Optional[int] = None,
    dataset: Optional[str] = None
) -> List[Tuple[str, str]]:
    """Apply offset and limit to dataset-resource pairs with validation.

    Args:
        dataset_resource_pairs: List of (dataset, resource) tuples
        offset: Optional offset to start from
        limit: Optional limit of pairs to return
        dataset: Optional dataset name (for error messages)

    Returns:
        Filtered list of (dataset, resource) tuples

    Raises:
        ValueError: If offset is beyond the total number of pairs
    """
    total_pairs = len(dataset_resource_pairs)

    if offset is not None:
        if offset >= total_pairs:
            error_msg = f"Offset {offset} is beyond the total number of transformation tasks ({total_pairs})"
            logger.error(error_msg)
            if dataset:
                logger.error(f"Note: Filtering by dataset '{dataset}'")
            raise ValueError(error_msg)
        dataset_resource_pairs = dataset_resource_pairs[offset:]

    if limit is not None:
        dataset_resource_pairs = dataset_resource_pairs[:limit]

    return dataset_resource_pairs


def load_state_resources(state_path: str, dataset: str) -> List[Tuple[str, str]]:
    """Load the ordered resource list for a dataset from a state.json file.

    Args:
        state_path: Path to state.json
        dataset: Dataset name to load resources for

    Returns:
        List of (dataset, resource) tuples in the order defined by state.json

    Raises:
        FileNotFoundError: If state_path does not exist
        KeyError: If state.json does not contain 'transform_resources' or dataset is not found
    """
    with open(state_path) as f:
        state = json.load(f)

    transform_resources = state["transform_resources"]

    if dataset not in transform_resources:
        raise KeyError(f"Dataset '{dataset}' not found in state.json transform_resources")

    return [(dataset, resource) for resource in transform_resources[dataset]]


def select_resources_to_process(
    dataset_resource_map: Dict[str, List[str]],
    dataset_resource_dir: str,
    pipeline_dir: str,
    specification_dir: str,
    dataset: Optional[str] = None,
    offset: Optional[int] = None,
    limit: Optional[int] = None,
    reprocess: bool = False,
    state_path: Optional[str] = None,
) -> List[Tuple[str, str]]:
    """Select the (dataset, resource) pairs to process or download.

    If state_path is provided, the stable ordered list from state.json is used
    as the base for offset/limit. Otherwise falls back to building from
    dataset_resource_map. The skip check (resource_needs_processing) is always
    applied after offset/limit so batch boundaries remain stable.

    1. Build the ordered list (from state.json if available, else dataset_resource_map)
    2. Apply offset/limit to get a stable batch slice
    3. If not reprocess: filter the slice to only those needing processing

    Args:
        dataset_resource_map: Dictionary mapping datasets to lists of resources
        dataset_resource_dir: Path to dataset resource logs
        pipeline_dir: Path to pipeline config directory (used for config hash)
        specification_dir: Path to specification directory (used for spec hash)
        dataset: Optional dataset name to filter to
        offset: Optional offset into the stable ordered list
        limit: Optional maximum number of pairs to return
        reprocess: If True, skip the dataset-resource log check
        state_path: Optional path to state.json for stable ordered resource list

    Returns:
        List of (dataset, resource) tuples to process
    """
    if state_path:
        if not dataset:
            raise ValueError("dataset is required when state_path is provided")
        pairs = load_state_resources(state_path, dataset=dataset)
    else:
        pairs = build_dataset_resource_pairs(dataset_resource_map, dataset=dataset)

    # Apply offset/limit to stable list first so batch boundaries never shift
    pairs = apply_offset_and_limit(pairs, offset=offset, limit=limit, dataset=dataset)

    if not reprocess:
        config_hash = hash_directory(pipeline_dir)
        specification_hash = hash_directory(specification_dir)
        before_skip = len(pairs)
        pairs = [
            (ds, resource) for ds, resource in pairs
            if resource_needs_processing(
                dataset_resource_dir, ds, resource,
                dl_version, config_hash, specification_hash,
            )
        ]
        skipped = before_skip - len(pairs)
        logger.info(
            f"Skipping {skipped} already up-to-date resources, "
            f"{len(pairs)} to process"
        )

    return pairs


def build_retired_resources_set(old_resource_entries: List[Dict]) -> set:
    """Build a set of retired resources (status 410).

    Args:
        old_resource_entries: List of old_resource entries from collection

    Returns:
        Set of retired resource IDs
    """
    retired_resources = set()
    for entry in old_resource_entries:
        if entry.get("status") == "410":
            retired_resources.add(entry["old-resource"])
    return retired_resources
