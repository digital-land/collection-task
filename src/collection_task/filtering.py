"""Filtering and resource management functions for collection tasks."""

import logging
from typing import Dict, List, Optional, Tuple

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
