import logging
import sys
import click

from digital_land.collection import Collection

from collection_task.downloading import download_files
from collection_task.filtering import (
    build_redirect_map,
    select_resources_to_process,
)

logger = logging.getLogger(__name__)


def download_resources(collection, collection_dir: str, bucket=None, base_url=None, collection_name=None, dataset=None, transformaiton_offset=None, transformation_limit=None, dataset_resource_dir="var/dataset-resource/", pipeline_dir="pipeline/", specification_dir="specification/", reprocess=False, max_threads=4) -> None:
    """Download resources for a collection.

    Uses the same selection logic as transform_resources (via select_resources_to_process)
    so only resources that will actually be transformed are downloaded.

    Args:
        collection: Unused, kept for backwards compatibility.
        collection_dir (str): The directory of the collection.
        bucket (str, optional): S3 bucket name to download from. If provided, will construct s3:// URLs.
        base_url (str, optional): Base URL for HTTP downloads (e.g., https://files.planning.data.gov.uk/).
        collection_name (str, optional): Collection name for URL construction. If not provided, will be inferred.
        dataset (str, optional): Filter resources to only this dataset.
        transformaiton_offset (int, optional): Offset for filtering resources.
        transformation_limit (int, optional): Limit for filtering resources.
        dataset_resource_dir (str, optional): Path to dataset resource logs (used for skip check).
        pipeline_dir (str, optional): Path to pipeline config (used for config hash).
        specification_dir (str, optional): Path to specification (used for specification hash).
        reprocess (bool, optional): If True, skip the dataset-resource log check and download all resources.
        max_threads (int, optional): Maximum number of concurrent download threads. Defaults to 4.
    """
    # Validate that either bucket or base_url is provided
    if not bucket and not base_url:
        error_msg = "Either --bucket or --base-url must be provided to download resources"
        logger.error(error_msg)
        raise ValueError(error_msg)

    collection = Collection(name=None, directory=collection_dir)
    collection.load()

    # Get collection name from collection object or parameter
    if not collection_name:
        collection_name = collection.name if hasattr(collection, 'name') and collection.name else "unknown"

    dataset_resource_map = collection.dataset_resource_map()

    redirect = build_redirect_map(collection.old_resource.entries)
    dataset_resource_pairs = select_resources_to_process(
        dataset_resource_map=dataset_resource_map,
        dataset_resource_dir=dataset_resource_dir,
        pipeline_dir=pipeline_dir,
        specification_dir=specification_dir,
        dataset=dataset,
        offset=transformaiton_offset,
        limit=transformation_limit,
        reprocess=reprocess,
    )

    # Extract unique resources to download (a resource only needs to be downloaded once)
    resources_to_download = list(set([res for _, res in dataset_resource_pairs]))

    logger.info(f"Downloading resources for {len(dataset_resource_pairs)} transformation tasks")

    # Build download map with URLs and output paths
    download_map = {}
    for old_resource in resources_to_download:
        # Get the actual resource to download (may be redirected)
        resource = redirect.get(old_resource, old_resource)

        # Skip resources that have been removed (redirect to empty)
        if not resource:
            logger.info(f"Skipping removed resource: {old_resource}")
            continue

        output_path = f"{collection_dir}/resource/{resource}"

        # Build URL based on provided option
        # Format: {bucket or base_url}/{collection}-collection/collection/resource/{resource}
        if bucket:
            # S3 bucket - construct s3:// URL
            url = f"s3://{bucket}/{collection_name}-collection/collection/resource/{resource}"
        elif base_url:
            # HTTP(S) base URL - construct full path
            # Ensure base_url ends with /
            base = base_url.rstrip('/') + '/'
            url = f"{base}{collection_name}-collection/collection/resource/{resource}"
        else:
            # This shouldn't happen due to validation above, but keep as fallback
            url = resource

        download_map[url] = output_path

    # Download all resources
    logger.info(f"Downloading {len(download_map)} resources...")
    download_files(download_map, max_threads=max_threads)


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
    help="S3 bucket name to download from (constructs s3:// URLs)"
)
@click.option(
    "--base-url",
    default=None,
    help="Base URL for HTTP(S) downloads (e.g., https://files.planning.data.gov.uk/)"
)
@click.option(
    "--collection-name",
    default=None,
    help="Collection name (e.g., 'brownfield-land'). If not provided, will be inferred from COLLECTION_NAME env var."
)
@click.option(
    "--dataset",
    default=None,
    help="Filter resources to only this dataset"
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
    "--dataset-resource-dir",
    default="var/dataset-resource/",
    help="Path to dataset resource logs (used to skip already up-to-date resources)"
)
@click.option(
    "--pipeline-dir",
    default="pipeline/",
    help="Path to the pipeline configuration directory (used for config hash)"
)
@click.option(
    "--specification-dir",
    default="specification/",
    help="Path to the specification directory (used for specification hash)"
)
@click.option(
    "--reprocess",
    is_flag=True,
    default=False,
    help="Download all resources, ignoring the dataset-resource skip check"
)
@click.option(
    "--max-threads",
    default=4,
    type=int,
    help="Maximum number of concurrent download threads"
)
@click.option(
    "--quiet",
    is_flag=True,
    help="Suppress progress output (only show warnings and errors)"
)
@click.option(
    "--debug",
    is_flag=True,
    help="Enable debug logging"
)
def run_command(collection_dir, bucket, base_url, collection_name, dataset, offset, limit, dataset_resource_dir, pipeline_dir, specification_dir, reprocess, max_threads, quiet, debug):
    """Download resources for a collection from S3 or HTTP(S) URLs.

    Either --bucket or --base-url must be provided.
    """
    # Configure logging
    if debug:
        logging.basicConfig(level=logging.DEBUG, format='%(levelname)s: %(message)s')
    elif quiet:
        logging.basicConfig(level=logging.WARNING, format='%(levelname)s: %(message)s')
    else:
        logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

    # Get collection name from parameter or environment variable
    import os
    if not collection_name:
        collection_name = os.environ.get('COLLECTION_NAME')
        if not collection_name:
            click.echo("Error: --collection-name must be provided or COLLECTION_NAME environment variable must be set", err=True)
            sys.exit(1)

    # Validate that either bucket or base_url is provided
    if not bucket and not base_url:
        click.echo("Error: Either --bucket or --base-url must be provided", err=True)
        raise click.Abort()

    try:
        # Download resources
        download_resources(
            collection=None,
            collection_dir=collection_dir,
            bucket=bucket,
            base_url=base_url,
            collection_name=collection_name,
            dataset=dataset,
            transformaiton_offset=offset,
            transformation_limit=limit,
            dataset_resource_dir=dataset_resource_dir,
            pipeline_dir=pipeline_dir,
            specification_dir=specification_dir,
            reprocess=reprocess,
            max_threads=max_threads,
        )
        click.echo("Download complete!")
    except ValueError as e:
        click.echo(f"\nError: {e}", err=True)
        sys.exit(1)
    except RuntimeError as e:
        click.echo(f"\nDownload failed: {e}", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"\nUnexpected error: {e}", err=True)
        sys.exit(1)


if __name__ == "__main__":
    run_command()