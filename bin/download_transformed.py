import logging
import sys
import click
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
from urllib.request import urlretrieve
from urllib.parse import urlparse

from digital_land.collection import Collection

try:
    import boto3
    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO3 = False

logger = logging.getLogger(__name__)


def download_file(url, output_path, raise_error=False, max_retries=5):
    """Downloads a file from an S3 or HTTPS URL.

    Automatically detects S3 URLs (s3://) and uses boto3 client.
    For HTTPS URLs, uses standard urlretrieve.

    Args:
        url: S3 URL (s3://bucket/key) or HTTPS URL
        output_path: Local path to save the file
        raise_error: Whether to raise exceptions or log them
        max_retries: Maximum number of retry attempts

    Returns:
        True if download succeeded, False otherwise
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Detect if this is an S3 URL
    parsed = urlparse(url)
    is_s3 = parsed.scheme == 's3'

    if is_s3:
        if not HAS_BOTO3:
            raise ImportError("boto3 is required for S3 downloads. Install it with: pip install boto3")

        # Parse S3 bucket and key from s3://bucket/key
        bucket = parsed.netloc
        key = parsed.path.lstrip('/')
        s3_client = boto3.client('s3')

    retries = 0
    while retries < max_retries:
        try:
            if is_s3:
                s3_client.download_file(bucket, key, str(output_path))
            else:
                urlretrieve(url, str(output_path))
            return True
        except Exception as e:
            if raise_error:
                raise e
            else:
                logger.error(f"error downloading file from {url}: {e}")
        retries += 1
    return False


def download_files(url_map, max_threads=4):
    """Orchestrates downloads across threads using a URL map.

    Args:
        url_map: Dictionary mapping URLs to local output paths {url: output_path}
        max_threads: Maximum number of concurrent download threads

    Raises:
        RuntimeError: If any downloads fail
    """
    use_progress_bar = sys.stdout.isatty()

    with ThreadPoolExecutor(max_threads) as executor:
        futures = {
            executor.submit(download_file, url, output_path): url
            for url, output_path in url_map.items()
        }
        results = []
        failed_downloads = []

        if use_progress_bar:
            iterator = tqdm(futures, desc="Downloading files")
        else:
            iterator = futures
            logger.info(f"Starting download of {len(futures)} files...")

        for i, future in enumerate(iterator, 1):
            url = futures[future]
            result = future.result()
            results.append(result)

            # Track failed downloads
            if not result:
                failed_downloads.append(url)

            if not use_progress_bar and i % 50 == 0:
                logger.info(f"Downloaded {i}/{len(futures)} files")

        if not use_progress_bar:
            logger.info(f"Completed download of {len(futures)} files")

        # Raise error if any downloads failed
        if failed_downloads:
            error_summary = f"Failed to download {len(failed_downloads)} file(s):\n" + "\n".join(failed_downloads)
            logger.error(error_summary)
            raise RuntimeError(error_summary)

    return results


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

    if bucket and not HAS_BOTO3:
        raise ImportError("boto3 is required for S3 downloads. Install it with: pip install boto3")

    # Build set of retired resources (status 410) from old_resource.csv
    retired_resources = set()
    if collection and hasattr(collection, 'old_resource'):
        for entry in collection.old_resource.entries:
            if entry.get("status") == "410":
                retired_resources.add(entry["old-resource"])

    # Build sorted list of (dataset, resource) pairs first
    # This preserves duplicates where a resource is used in multiple datasets
    datasets_to_process = [dataset] if dataset else sorted(dataset_resource_map.keys())

    dataset_resource_pairs = []
    for ds in sorted(datasets_to_process):
        if ds not in dataset_resource_map:
            if dataset:  # Only raise error if user explicitly requested this dataset
                raise ValueError(f"Dataset '{dataset}' not found in dataset_resource_map")
            continue
        for resource in sorted(dataset_resource_map[ds]):
            dataset_resource_pairs.append((ds, resource))

    # Store total count before applying offset/limit
    total_pairs = len(dataset_resource_pairs)

    # Apply offset and limit to the transformation task list BEFORE considering retired resources
    if transformation_offset is not None:
        if transformation_offset >= total_pairs:
            error_msg = f"Offset {transformation_offset} is beyond the total number of transformation tasks ({total_pairs})"
            logger.error(error_msg)
            if dataset:
                logger.error(f"Note: Filtering by dataset '{dataset}'")
            raise ValueError(error_msg)
        dataset_resource_pairs = dataset_resource_pairs[transformation_offset:]

    if transformation_limit is not None:
        dataset_resource_pairs = dataset_resource_pairs[:transformation_limit]

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
