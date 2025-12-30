import logging
import sys
import click
from pathlib import Path
from multiprocessing import Pool, cpu_count
from tqdm import tqdm

from digital_land.collection import Collection
from digital_land.pipeline import Pipeline
from digital_land.specification import Specification
from digital_land.commands import pipeline_run

logger = logging.getLogger(__name__)


def process_single_resource(args):
    """Process a single resource through the digital-land pipeline using Python library directly.

    Args:
        args: Tuple of (old_resource, dataset, resource_path, endpoints, organisations, entry_date, config)
              where old_resource is the original resource identifier (used for output filename)
              and resource_path points to the actual file (may be redirected)

    Returns:
        Tuple of (old_resource, success, error_message)
    """
    old_resource, dataset, resource_path, endpoints, organisations, entry_date, config = args

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

        # Call the pipeline_run function directly instead of subprocess
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
    cache_dir="var/cache/",
    transformed_dir="transformed/",
    issue_dir="issue/",
    operational_issue_dir="performance/operational_issue/",
    output_log_dir="log/",
    column_field_dir="var/column-field/",
    dataset_resource_dir="var/dataset-resource/",
    converted_resource_dir="var/converted-resource/",
    offset=None,
    limit=None,
    max_workers=None
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
        offset: Optional offset for filtering resources
        limit: Optional limit for filtering resources
        max_workers: Number of worker processes (defaults to CPU count)
    """
    # Load collection
    collection = Collection(name=None, directory=collection_dir)
    collection.load()
    dataset_resource_map = collection.dataset_resource_map()

    # Build redirect map from old_resource.csv
    redirect = {}
    for entry in collection.old_resource.entries:
        redirect[entry["old-resource"]] = entry["resource"]

    # Get sorted list of all resources
    sorted_resource_list = []
    for key in sorted(dataset_resource_map.keys()):
        sorted_resource_list.extend(sorted(dataset_resource_map[key]))

    # Apply offset and limit
    filtered_resources = sorted_resource_list
    if offset is not None:
        filtered_resources = filtered_resources[offset:]
    if limit is not None:
        filtered_resources = filtered_resources[:limit]

    resources_to_process = list(set(filtered_resources))

    logger.info(f"Processing {len(resources_to_process)} unique resources across {len(dataset_resource_map)} datasets")

    # Build task list
    tasks = []
    for dataset in sorted(dataset_resource_map.keys()):
        for old_resource in sorted(dataset_resource_map[dataset]):
            if old_resource not in resources_to_process:
                continue

            # Get the actual resource to process (may be redirected)
            resource = redirect.get(old_resource, old_resource)

            # Skip resources that have been removed (redirect to empty)
            if not resource:
                logger.info(f"Skipping removed resource: {old_resource}")
                continue

            # Use the redirected resource path, but old_resource for metadata
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

            # If resource was redirected, include the old_resource in config
            if resource != old_resource:
                config['resource'] = old_resource

            tasks.append((old_resource, dataset, resource_path, endpoints, organisations, entry_date, config))

    if not tasks:
        logger.warning("No resources to process")
        return

    # Determine number of workers
    if max_workers is None:
        max_workers = cpu_count()

    logger.info(f"Using {max_workers} worker processes")

    # Process resources in parallel
    use_progress_bar = sys.stdout.isatty()

    successful = 0
    failed = 0
    errors = []

    with Pool(processes=max_workers) as pool:
        if use_progress_bar:
            # Interactive mode with progress bar
            results = list(tqdm(
                pool.imap(process_single_resource, tasks),
                total=len(tasks),
                desc="Processing resources"
            ))
        else:
            # Non-interactive mode with periodic logging
            results = []
            logger.info(f"Starting processing of {len(tasks)} resources...")
            for i, result in enumerate(pool.imap(process_single_resource, tasks), 1):
                results.append(result)
                if i % 10 == 0:
                    logger.info(f"Processed {i}/{len(tasks)} resources")
            logger.info(f"Completed processing of {len(tasks)} resources")

    # Collect results
    for resource, success, error_msg in results:
        if success:
            successful += 1
        else:
            failed += 1
            errors.append((resource, error_msg))

    # Log summary
    logger.info(f"Processing complete: {successful} successful, {failed} failed")

    if errors:
        logger.error(f"Failed resources:")
        for resource, error_msg in errors:
            logger.error(f"  - {resource}: {error_msg}")

    return successful, failed, errors


@click.command()
@click.option(
    "--collection-dir",
    required=True,
    type=click.Path(exists=True),
    help="Path to the collection directory"
)
@click.option(
    "--pipeline-dir",
    default="pipeline/",
    help="Path to the pipeline configuration directory"
)
@click.option(
    "--cache-dir",
    default="var/cache/",
    help="Path to the cache directory"
)
@click.option(
    "--transformed-dir",
    default="transformed/",
    help="Path to the transformed output directory"
)
@click.option(
    "--issue-dir",
    default="issue/",
    help="Path to the issue directory"
)
@click.option(
    "--operational-issue-dir",
    default="performance/operational_issue/",
    help="Path to the operational issue directory"
)
@click.option(
    "--output-log-dir",
    default="log/",
    help="Path to the output log directory"
)
@click.option(
    "--column-field-dir",
    default="var/column-field/",
    help="Path to the column field directory"
)
@click.option(
    "--dataset-resource-dir",
    default="var/dataset-resource/",
    help="Path to the dataset resource directory"
)
@click.option(
    "--converted-resource-dir",
    default="var/converted-resource/",
    help="Path to the converted resource directory"
)
@click.option(
    "--offset",
    default=None,
    type=int,
    help="Offset for filtering resources"
)
@click.option(
    "--limit",
    default=None,
    type=int,
    help="Limit for filtering resources"
)
@click.option(
    "--max-workers",
    default=None,
    type=int,
    help="Number of worker processes (defaults to CPU count)"
)
@click.option(
    "--verbose",
    is_flag=True,
    help="Enable verbose logging"
)
def run_command(
    collection_dir,
    pipeline_dir,
    cache_dir,
    transformed_dir,
    issue_dir,
    operational_issue_dir,
    output_log_dir,
    column_field_dir,
    dataset_resource_dir,
    converted_resource_dir,
    offset,
    limit,
    max_workers,
    verbose
):
    """Process resources using multiprocessing instead of make."""
    # Configure logging
    if verbose:
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')
    else:
        logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s: %(message)s')

    # Process resources
    successful, failed, errors = process_resources(
        collection_dir=collection_dir,
        pipeline_dir=pipeline_dir,
        cache_dir=cache_dir,
        transformed_dir=transformed_dir,
        issue_dir=issue_dir,
        operational_issue_dir=operational_issue_dir,
        output_log_dir=output_log_dir,
        column_field_dir=column_field_dir,
        dataset_resource_dir=dataset_resource_dir,
        converted_resource_dir=converted_resource_dir,
        offset=offset,
        limit=limit,
        max_workers=max_workers
    )

    click.echo(f"\nProcessing complete!")
    click.echo(f"Successful: {successful}")
    click.echo(f"Failed: {failed}")

    if failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    run_command()
