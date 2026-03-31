"""Transform functions for processing collection resources through the pipeline."""

import logging
import sys
from pathlib import Path
from multiprocessing import Pool, cpu_count
from tqdm import tqdm

from digital_land.collection import Collection
from digital_land.pipeline import Pipeline
from digital_land.specification import Specification
from digital_land.commands import pipeline_run

from collection_task.filtering import (
    build_redirect_map,
    select_resources_to_process,
)

logger = logging.getLogger(__name__)


def process_single_resource(args):
    """Process a single resource through the digital-land pipeline.

    Args:
        args: Tuple of (old_resource, dataset, resource_path, endpoints, organisations, entry_date, config)
              where old_resource is the original resource identifier (used for output filename)
              and resource_path points to the actual file (may be redirected)

    Returns:
        Tuple of (old_resource, success, error_message)

    Raises:
        FileNotFoundError: If the resource file does not exist at resource_path
    """
    old_resource, dataset, resource_path, endpoints, organisations, entry_date, config = args

    if not Path(resource_path).exists():
        raise FileNotFoundError(
            f"Resource file not found for {old_resource} (dataset={dataset}): {resource_path}"
        )

    try:
        # Build output directories
        transformed_dir = Path(config['transformed_dir']) / dataset
        issue_dir = Path(config['issue_dir']) / dataset
        operational_issue_dir = Path(config['operational_issue_dir'])
        output_log_dir = Path(config['output_log_dir'])
        column_field_dir = Path(config['column_field_dir']) / dataset
        dataset_resource_dir = Path(config['dataset_resource_dir']) / dataset
        converted_resource_dir = Path(config['converted_resource_dir']) / dataset

        # Create directories
        for directory in [transformed_dir, issue_dir, operational_issue_dir,
                          output_log_dir, column_field_dir, dataset_resource_dir,
                          converted_resource_dir]:
            directory.mkdir(parents=True, exist_ok=True)

        # Build output path using old_resource (original resource identifier)
        output_path = transformed_dir / f"{old_resource}.csv"

        # Initialize pipeline and specification objects
        pipeline = Pipeline(path=config['pipeline_dir'], dataset=dataset)
        specification = Specification(config.get('specification_dir', 'specification/'))

        # Parse endpoints and organisations (convert space-separated strings to lists)
        endpoints_list = endpoints.split() if endpoints else []
        organisations_list = organisations.split() if organisations else []

        pipeline_run(
            dataset=dataset,
            pipeline=pipeline,
            specification=specification,
            input_path=str(resource_path),
            output_path=output_path,
            collection_dir=config.get('collection_dir', 'collection/'),
            issue_dir=str(issue_dir),
            operational_issue_dir=str(operational_issue_dir),
            column_field_dir=str(column_field_dir),
            dataset_resource_dir=str(dataset_resource_dir),
            converted_resource_dir=str(converted_resource_dir),
            organisation_path=config['organisation_path'],
            config_path=config['config_path'],
            endpoints=endpoints_list,
            organisations=organisations_list,
            entry_date=entry_date,
            cache_dir=config.get('cache_dir', 'var/cache'),
            resource=config.get('resource'),  # For redirected resources
            output_log_dir=str(output_log_dir),
        )

        return (old_resource, True, None)

    except Exception as e:
        error_msg = str(e)
        logger.error(f"Error processing {old_resource} for dataset {dataset}: {error_msg}")
        return (old_resource, False, error_msg)


def process_resources(
    collection_dir,
    pipeline_dir="pipeline/",
    specification_dir="specification/",
    cache_dir="var/cache/",
    transformed_dir="transformed/",
    issue_dir="issue/",
    operational_issue_dir="performance/operational_issue/",
    output_log_dir="log/",
    column_field_dir="var/column-field/",
    dataset_resource_dir="var/dataset-resource/",
    converted_resource_dir="var/converted-resource/",
    dataset=None,
    offset=None,
    limit=None,
    max_workers=None,
    reprocess=False,
    state_path=None,
):
    """Process resources using multiprocessing.

    Args:
        collection_dir: Path to the collection directory
        pipeline_dir: Path to the pipeline configuration directory
        cache_dir: Path to the cache directory
        transformed_dir: Path to the transformed output directory
        issue_dir: Path to the issue directory
        operational_issue_dir: Path to the operational issue directory
        output_log_dir: Path to the output log directory
        column_field_dir: Path to the column field directory
        dataset_resource_dir: Path to the dataset resource directory
        converted_resource_dir: Path to the converted resource directory
        dataset: Optional dataset filter - only process resources from this dataset
        offset: Optional offset for filtering resources
        limit: Optional limit for filtering resources
        max_workers: Number of worker processes (defaults to CPU count)
        reprocess: If True, skip the dataset-resource log check and reprocess all resources
    """
    collection = Collection(name=None, directory=collection_dir)
    collection.load()
    dataset_resource_map = collection.dataset_resource_map()
    total_pairs = len(dataset_resource_map)

    redirect = build_redirect_map(collection.old_resource.entries)
    dataset_resource_pairs = select_resources_to_process(
        dataset_resource_map=dataset_resource_map,
        dataset_resource_dir=dataset_resource_dir,
        pipeline_dir=pipeline_dir,
        specification_dir=specification_dir,
        dataset=dataset,
        offset=offset,
        limit=limit,
        reprocess=reprocess,
        state_path=state_path,
    )

    tasks = []
    for ds, old_resource in dataset_resource_pairs:
        resource = redirect.get(old_resource, old_resource)

        if not resource:
            logger.info(f"Skipping removed resource: {old_resource}")
            continue

        resource_path = collection.resource_path(resource)
        endpoints = " ".join(collection.resource_endpoints(old_resource))
        organisations = " ".join(collection.resource_organisations(old_resource))
        entry_date = collection.resource_start_date(old_resource)

        config = {
            'pipeline_dir': pipeline_dir,
            'cache_dir': cache_dir,
            'collection_dir': collection_dir,
            'transformed_dir': transformed_dir,
            'issue_dir': issue_dir,
            'operational_issue_dir': operational_issue_dir,
            'output_log_dir': output_log_dir,
            'column_field_dir': column_field_dir,
            'dataset_resource_dir': dataset_resource_dir,
            'converted_resource_dir': converted_resource_dir,
            'config_path': f"{cache_dir}config.sqlite3",
            'organisation_path': f"{cache_dir}organisation.csv",
        }

        if resource != old_resource:
            config['resource'] = old_resource

        tasks.append((old_resource, ds, resource_path, endpoints, organisations, entry_date, config))

    logger.info(f"Processing {len(tasks)} transformation tasks (out of {total_pairs} total)")

    if not tasks:
        logger.warning("No transformation tasks to process after applying filters")
        return

    if max_workers is None:
        max_workers = cpu_count()

    logger.info(f"Using {max_workers} worker processes")

    use_progress_bar = sys.stdout.isatty()
    successful = 0
    failed = 0
    errors = []

    with Pool(processes=max_workers) as pool:
        if use_progress_bar:
            results = list(tqdm(
                pool.imap(process_single_resource, tasks),
                total=len(tasks),
                desc="Processing resources"
            ))
        else:
            results = []
            total_tasks = len(tasks)
            logger.info(f"Starting processing of {total_tasks} transformation tasks...")

            progress_interval = max(1, total_tasks // 10)
            last_logged_percent = 0

            for i, result in enumerate(pool.imap(process_single_resource, tasks), 1):
                results.append(result)

                current_percent = (i * 100) // total_tasks
                if current_percent >= last_logged_percent + 10 or i == total_tasks:
                    logger.info(f"Progress: {i}/{total_tasks} tasks ({current_percent}%)")
                    last_logged_percent = current_percent

            logger.info(f"Completed processing of {total_tasks} transformation tasks")

    for resource, success, error_msg in results:
        if success:
            successful += 1
        else:
            failed += 1
            errors.append((resource, error_msg))

    logger.info(f"Processing complete: {successful} successful, {failed} failed")

    if errors:
        logger.error("Failed resources:")
        for resource, error_msg in errors:
            logger.error(f"  - {resource}: {error_msg}")

    return successful, failed, errors
