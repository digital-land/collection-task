import logging
import sys
import click
from concurrent.futures import ThreadPoolExecutor

from digital_land.collection import Collection

from collection_task.downloading import download_file
from collection_task.filtering import build_dataset_resource_pairs

logger = logging.getLogger(__name__)


def download_dataset_resource(
    collection_dir,
    bucket=None,
    base_url=None,
    collection_name=None,
    dataset_resource_dir="var/dataset-resource/",
    dataset=None,
    max_threads=4,
):
    """Download dataset resource log files for all resources in the collection.

    These small CSVs record the code version, config hash, and specification hash
    used when each resource was last processed. They are used to determine whether
    a resource needs reprocessing on subsequent runs.

    404s for resources that have never been processed are expected and ignored.

    Args:
        collection_dir: Path to the collection directory
        bucket: S3 bucket name (optional if base_url provided)
        base_url: Base HTTP(S) URL (optional if bucket provided)
        collection_name: Collection name used in URL construction
        dataset_resource_dir: Local directory to store dataset resource logs
        dataset: Optional dataset name to filter downloads
        max_threads: Maximum concurrent download threads

    Returns:
        Tuple of (downloaded_count, not_found_count)
    """
    if not bucket and not base_url:
        raise ValueError("Either bucket or base_url must be provided")

    collection = Collection(name=None, directory=collection_dir)
    collection.load()

    dataset_resource_map = collection.dataset_resource_map()
    pairs = build_dataset_resource_pairs(dataset_resource_map, dataset=dataset)

    logger.info(f"Downloading dataset resource logs for {len(pairs)} resources...")

    # Build URL -> local path map
    url_map = {}
    for ds, resource in pairs:
        local_path = f"{dataset_resource_dir}{ds}/{resource}.csv"
        remote_path = f"{dataset_resource_dir}{ds}/{resource}.csv"

        if bucket:
            url = f"s3://{bucket}/{collection_name}-collection/{remote_path}"
        else:
            base = base_url.rstrip("/") + "/"
            url = f"{base}{collection_name}-collection/{remote_path}"

        url_map[url] = local_path

    # Download concurrently using download_file directly (not download_files) so
    # that 404s for new/unprocessed resources don't raise an error.
    downloaded = 0
    with ThreadPoolExecutor(max_threads) as executor:
        futures = {
            executor.submit(download_file, url, path, raise_error=False, max_retries=1): url
            for url, path in url_map.items()
        }
        for future in futures:
            if future.result():
                downloaded += 1

    not_found = len(pairs) - downloaded
    logger.info(
        f"Downloaded {downloaded} dataset resource logs "
        f"({not_found} not found - these resources will be processed)"
    )

    return downloaded, not_found


@click.command()
@click.option(
    "--collection-dir",
    required=True,
    type=click.Path(exists=True),
    help="Path to the collection directory",
)
@click.option(
    "--bucket",
    default=None,
    help="S3 bucket name to download from",
)
@click.option(
    "--base-url",
    default=None,
    help="Base URL for HTTP(S) downloads (e.g., https://files.planning.data.gov.uk/)",
)
@click.option(
    "--collection-name",
    required=True,
    help="Collection name (e.g., 'brownfield-land')",
)
@click.option(
    "--dataset-resource-dir",
    default="var/dataset-resource/",
    help="Local directory to store dataset resource logs",
)
@click.option(
    "--dataset",
    default=None,
    help="Filter downloads to only this dataset",
)
@click.option(
    "--max-threads",
    default=4,
    type=int,
    help="Maximum number of concurrent download threads",
)
@click.option(
    "--quiet",
    is_flag=True,
    help="Suppress progress output (only show warnings and errors)",
)
@click.option(
    "--debug",
    is_flag=True,
    help="Enable debug logging",
)
def run_command(
    collection_dir,
    bucket,
    base_url,
    collection_name,
    dataset_resource_dir,
    dataset,
    max_threads,
    quiet,
    debug,
):
    """Download dataset resource logs from S3 or HTTP(S).

    Run this before calculating state (for accurate transform counts) and before
    transforming (to skip already up-to-date resources).

    Either --bucket or --base-url must be provided.
    """
    if debug:
        logging.basicConfig(level=logging.DEBUG, format="%(levelname)s: %(message)s")
    elif quiet:
        logging.basicConfig(level=logging.WARNING, format="%(levelname)s: %(message)s")
    else:
        logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    if not bucket and not base_url:
        click.echo("Error: Either --bucket or --base-url must be provided", err=True)
        sys.exit(1)

    try:
        downloaded, not_found = download_dataset_resource(
            collection_dir=collection_dir,
            bucket=bucket,
            base_url=base_url,
            collection_name=collection_name,
            dataset_resource_dir=dataset_resource_dir,
            dataset=dataset,
            max_threads=max_threads,
        )
        click.echo(
            f"Done: {downloaded} logs downloaded, {not_found} not found (will be processed)"
        )
    except ValueError as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"Unexpected error: {e}", err=True)
        sys.exit(1)


if __name__ == "__main__":
    run_command()
