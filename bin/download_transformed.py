import logging
import sys
import click

from digital_land.collection import Collection

from collection_task.downloading import download_files
from collection_task.filtering import (
    build_retired_resources_set,
    build_dataset_resource_pairs,
    apply_offset_and_limit,
)

logger = logging.getLogger(__name__)


def download_transformed(
    dataset_resource_map,
    collection=None,
    bucket=None,
    base_url=None,
    collection_name=None,
    dataset=None,
    transformed_dir="transformed/",
    issue_dir="issue/",
    column_field_dir="var/column-field/",
    dataset_resource_dir="var/dataset-resource/",
    converted_resource_dir="var/converted-resource/",
    max_threads=4,
    transformation_offset=None,
    transformation_limit=None
):
    """Download transformed resources using dataset_resource_map.

    Creates a URL map from the dataset_resource_map and passes it to download_files.

    Args:
        dataset_resource_map: Dictionary mapping datasets to lists of resources
        collection: Collection object to access old_resource data
        bucket: S3 bucket name (optional if base_url provided)
        base_url: Base URL for HTTP(S) downloads (optional if bucket provided)
        collection_name: Collection name (e.g., 'brownfield-land')
        dataset: Optional dataset name to filter downloads (only download resources for this dataset)
        transformed_dir: Local directory for transformed files
        issue_dir: Local directory for issue files
        column_field_dir: Local directory for column field mappings
        dataset_resource_dir: Local directory for dataset resource mappings
        converted_resource_dir: Local directory for converted resources
        max_threads: Maximum concurrent downloads
        transformation_offset: Optional offset for filtering resources
        transformation_limit: Optional limit for filtering resources

    Returns:
        List of boolean results indicating success/failure for each download
    """
    # Validate that either bucket or base_url is provided
    if not bucket and not base_url:
        raise ValueError("Either bucket or base_url must be provided")

    retired_resources = set()
    if collection and hasattr(collection, 'old_resource'):
        retired_resources = build_retired_resources_set(collection.old_resource.entries)

    dataset_resource_pairs = build_dataset_resource_pairs(dataset_resource_map, dataset=dataset)
    total_pairs = len(dataset_resource_pairs)
    dataset_resource_pairs = apply_offset_and_limit(
        dataset_resource_pairs,
        offset=transformation_offset,
        limit=transformation_limit,
        dataset=dataset,
    )

    logger.info(f"Downloading transformed files for {len(dataset_resource_pairs)} transformation tasks (out of {total_pairs} total)")

    # Now build URL map from the filtered list
    url_map = {}

    for ds, resource in dataset_resource_pairs:
        # Skip retired resources (status 410)
        if resource in retired_resources:
            logger.info(f"Skipping retired resource (status 410): {resource}")
            continue

        # Define all file types to download per resource
        # Files are organized by dataset, not just resource
        files_to_download = [
            (f"{transformed_dir}{ds}/{resource}.parquet", f"{transformed_dir}{ds}/{resource}.parquet"),
            (f"{issue_dir}{ds}/{resource}.csv", f"{issue_dir}{ds}/{resource}.csv"),
            (f"{column_field_dir}{ds}/{resource}.csv", f"{column_field_dir}{ds}/{resource}.csv"),
            (f"{dataset_resource_dir}{ds}/{resource}.csv", f"{dataset_resource_dir}{ds}/{resource}.csv"),
            (f"{converted_resource_dir}{ds}/{resource}.csv", f"{converted_resource_dir}{ds}/{resource}.csv"),
        ]

        for local_path, remote_path in files_to_download:
            if bucket:
                # Build S3 URL
                url = f"s3://{bucket}/{collection_name}-collection/{remote_path}"
            else:
                # Build HTTP(S) URL
                base = base_url.rstrip('/') + '/'
                url = f"{base}{collection_name}-collection/{remote_path}"

            url_map[url] = local_path

    # Log download info
    source_type = "S3" if bucket else "HTTP(S)"
    num_transformation_tasks = len(dataset_resource_pairs) - len([r for _, r in dataset_resource_pairs if r in retired_resources])
    files_per_task = len(url_map) // num_transformation_tasks if num_transformation_tasks > 0 else 0
    logger.info(f"Downloading {len(url_map)} files ({files_per_task} per transformation task) from {source_type}...")

    # Download files
    results = download_files(url_map, max_threads=max_threads)

    logger.info("Download complete!")
    return results


def download_transformed_resources(
    collection_dir: str,
    bucket: str = None,
    base_url: str = None,
    collection_name: str = None,
    dataset: str = None,
    transformed_dir: str = "transformed/",
    issue_dir: str = "issue/",
    column_field_dir: str = "var/column-field/",
    dataset_resource_dir: str = "var/dataset-resource/",
    converted_resource_dir: str = "var/converted-resource/",
    max_threads: int = 4,
    transformation_offset: int = None,
    transformation_limit: int = None
) -> None:
    """Download transformed resources and related files from S3 or HTTP(S) URLs.

    Args:
        collection_dir: Local collection directory
        bucket: S3 bucket name (optional if base_url provided)
        base_url: Base URL for HTTP(S) downloads (optional if bucket provided)
        collection_name: Collection name (e.g., 'brownfield-land')
        dataset: Optional dataset name to filter downloads
        transformed_dir: Local directory for transformed files
        issue_dir: Local directory for issue files
        column_field_dir: Local directory for column field mappings
        dataset_resource_dir: Local directory for dataset resource mappings
        converted_resource_dir: Local directory for converted resources
        max_threads: Maximum concurrent downloads
        transformation_offset: Optional offset for filtering resources
        transformation_limit: Optional limit for filtering resources
    """
    # Load collection to get resource list
    collection = Collection(name=None, directory=collection_dir)
    collection.load()

    # Get dataset_resource_map and delegate to download_transformed
    dataset_resource_map = collection.dataset_resource_map()

    download_transformed(
        dataset_resource_map=dataset_resource_map,
        collection=collection,
        bucket=bucket,
        base_url=base_url,
        collection_name=collection_name,
        dataset=dataset,
        transformed_dir=transformed_dir,
        issue_dir=issue_dir,
        column_field_dir=column_field_dir,
        dataset_resource_dir=dataset_resource_dir,
        converted_resource_dir=converted_resource_dir,
        max_threads=max_threads,
        transformation_offset=transformation_offset,
        transformation_limit=transformation_limit
    )


@click.command()
@click.option(
    "--collection-dir",
    required=True,
    type=click.Path(exists=True),
    help="Path to the collection directory"
)
@click.option(
    "--bucket",
    default=None,
    help="S3 bucket name to download from (optional if --base-url provided)"
)
@click.option(
    "--base-url",
    default=None,
    help="Base URL for HTTP(S) downloads (e.g., https://files.planning.data.gov.uk/)"
)
@click.option(
    "--collection-name",
    required=True,
    help="Collection name (e.g., 'brownfield-land')"
)
@click.option(
    "--dataset",
    default=None,
    help="Optional dataset name to filter downloads (e.g., 'brownfield-land')"
)
@click.option(
    "--transformed-dir",
    default="transformed/",
    help="Local directory for transformed files"
)
@click.option(
    "--issue-dir",
    default="issue/",
    help="Local directory for issue files"
)
@click.option(
    "--column-field-dir",
    default="var/column-field/",
    help="Local directory for column field mappings"
)
@click.option(
    "--dataset-resource-dir",
    default="var/dataset-resource/",
    help="Local directory for dataset resource mappings"
)
@click.option(
    "--converted-resource-dir",
    default="var/converted-resource/",
    help="Local directory for converted resources"
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
    "--max-threads",
    default=4,
    type=int,
    help="Maximum number of concurrent download threads"
)
@click.option(
    "--verbose",
    is_flag=True,
    help="Enable verbose logging"
)
def run_command(
    collection_dir,
    bucket,
    base_url,
    collection_name,
    dataset,
    transformed_dir,
    issue_dir,
    column_field_dir,
    dataset_resource_dir,
    converted_resource_dir,
    offset,
    limit,
    max_threads,
    verbose
):
    """Download transformed resources and supporting files from S3 or HTTP(S) URLs.

    Either --bucket or --base-url must be provided.
    """
    # Configure logging
    if verbose:
        logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
    else:
        logging.basicConfig(level=logging.WARNING, format='%(levelname)s: %(message)s')

    # Validate that either bucket or base_url is provided
    if not bucket and not base_url:
        click.echo("Error: Either --bucket or --base-url must be provided", err=True)
        sys.exit(1)

    try:
        download_transformed_resources(
            collection_dir=collection_dir,
            bucket=bucket,
            base_url=base_url,
            collection_name=collection_name,
            dataset=dataset,
            transformed_dir=transformed_dir,
            issue_dir=issue_dir,
            column_field_dir=column_field_dir,
            dataset_resource_dir=dataset_resource_dir,
            converted_resource_dir=converted_resource_dir,
            max_threads=max_threads,
            transformation_offset=offset,
            transformation_limit=limit
        )
        click.echo("Download complete!")
    except RuntimeError as e:
        click.echo(f"\nDownload failed: {e}", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"\nUnexpected error: {e}", err=True)
        sys.exit(1)


if __name__ == "__main__":
    run_command()
