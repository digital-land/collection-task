import logging
import sys
import click
from tqdm import tqdm

from pathlib import Path
from urllib.request import urlretrieve
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor

from digital_land.collection import Collection

try:
    import boto3
    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO3 = False

logger = logging.getLogger("__name__")


def download_file(url, output_path, raise_error=False, max_retries=5):
    """Downloads a file using urllib for HTTP(S) URLs or boto3 for S3 URLs."""
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Check if this is an s3:// URL
    if url.startswith('s3://'):
        if not HAS_BOTO3:
            error_msg = "boto3 is required to download from s3:// URLs. Install it with: pip install boto3"
            logger.error(error_msg)
            if raise_error:
                raise ImportError(error_msg)
            return

        parsed = urlparse(url)
        bucket = parsed.netloc
        key = parsed.path.lstrip('/')

        retries = 0
        while retries < max_retries:
            try:
                s3 = boto3.client('s3')
                s3.download_file(bucket, key, str(output_path))
                break
            except Exception as e:
                if raise_error:
                    raise e
                else:
                    logger.error(f"error downloading file from S3 url {url}: {e}")
            retries += 1
    else:
        # Use urllib for HTTP(S) URLs (including https://s3.amazonaws.com/... URLs)
        retries = 0
        while retries < max_retries:
            try:
                urlretrieve(url, output_path)
                break
            except Exception as e:
                if raise_error:
                    raise e
                else:
                    logger.error(f"error downloading file from url {url}: {e}")
            retries += 1


def download_urls(url_map, max_threads=4):
    """Downloads multiple files concurrently using threads.

    Raises:
        RuntimeError: If any downloads fail
    """

    # Only use tqdm if we're in an interactive terminal
    use_progress_bar = sys.stdout.isatty()

    with ThreadPoolExecutor(max_threads) as executor:
        futures = {
            executor.submit(download_file, url, output_path): url
            for url, output_path in url_map.items()
        }
        results = []
        errors = []
        total_files = len(futures)

        # Use tqdm for interactive terminals, plain iteration for cloud/non-interactive
        if use_progress_bar:
            iterator = tqdm(futures, desc="Downloading files")
        else:
            iterator = futures
            logger.info(f"Starting download of {total_files} files...")

        last_logged_percent = 0

        for i, future in enumerate(iterator, 1):
            try:
                results.append(future.result())
                # Log progress at 10% intervals in non-interactive mode
                if not use_progress_bar:
                    current_percent = (i * 100) // total_files
                    if current_percent >= last_logged_percent + 10 or i == total_files:
                        logger.info(f"Progress: {i}/{total_files} files ({current_percent}%)")
                        last_logged_percent = current_percent
            except Exception as e:
                url = futures[future]
                error_msg = f"Failed to download {url}: {e}"
                logger.error(error_msg)
                errors.append(error_msg)

        if not use_progress_bar:
            logger.info(f"Completed download of {total_files} files")

        # Raise an error if any downloads failed
        if errors:
            error_summary = f"Failed to download {len(errors)} file(s):\n" + "\n".join(errors)
            logger.error(error_summary)
            raise RuntimeError(error_summary)

        return results

def download_resources(collection, collection_dir: str, bucket=None, base_url=None, collection_name=None, dataset=None, transformaiton_offset=None, transformation_limit=None, max_threads=4) -> None:
    """Download resources for a collection., can limit and offset based on how many. transformation
    rresource numberr may differ to
    Args:
        collection: The collection object or None to load from collection_dir
        collection_dir (str): The directory of the collection.
        bucket (str, optional): S3 bucket name to download from. If provided, will construct s3:// URLs.
        base_url (str, optional): Base URL for HTTP downloads (e.g., https://files.planning.data.gov.uk/).
        collection_name (str, optional): Collection name for URL construction. If not provided, will be inferred.
        dataset (str, optional): Filter resources to only this dataset.
        transformaiton_offset (int, optional): Offset for filtering resources.
        transformation_limit (int, optional): Limit for filtering resources.
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

    # Build redirect map from old_resource.csv
    redirect = {}
    for entry in collection.old_resource.entries:
        redirect[entry["old-resource"]] = entry["resource"]

    # Build list of (dataset, resource) pairs in order
    # This preserves duplicates where a resource is used in multiple datasets
    datasets_to_process = [dataset] if dataset else sorted(dataset_resource_map.keys())

    dataset_resource_pairs = []
    for key in sorted(datasets_to_process):
        if key in dataset_resource_map:
            for res in sorted(dataset_resource_map[key]):
                dataset_resource_pairs.append((key, res))

    # Store total count before applying offset/limit
    total_pairs = len(dataset_resource_pairs)

    # Apply offset and limit to the actual download tasks
    if transformaiton_offset is not None:
        if transformaiton_offset >= total_pairs:
            error_msg = f"Offset {transformaiton_offset} is beyond the total number of transformation tasks ({total_pairs})"
            logger.error(error_msg)
            if dataset:
                logger.error(f"Note: Filtering by dataset '{dataset}'")
            raise ValueError(error_msg)
        dataset_resource_pairs = dataset_resource_pairs[transformaiton_offset:]

    if transformation_limit is not None:
        dataset_resource_pairs = dataset_resource_pairs[:transformation_limit]

    # Extract unique resources to download (a resource only needs to be downloaded once)
    resources_to_download = list(set([res for _, res in dataset_resource_pairs]))

    logger.info(f"Downloading resources for {len(dataset_resource_pairs)} transformation tasks (out of {total_pairs} total)")

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
    download_urls(download_map, max_threads=max_threads)


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
def run_command(collection_dir, bucket, base_url, collection_name, dataset, offset, limit, max_threads, quiet, debug):
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
            max_threads=max_threads
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