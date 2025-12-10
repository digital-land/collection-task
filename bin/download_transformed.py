import logging
import sys
import click
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm

from digital_land.collection import Collection

try:
    import boto3
    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO3 = False

logger = logging.getLogger(__name__)


def download_s3_file(s3_client, bucket, key, output_path, raise_error=False, max_retries=5):
    """Downloads a file from S3."""
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    retries = 0
    while retries < max_retries:
        try:
            s3_client.download_file(bucket, key, str(output_path))
            return True
        except Exception as e:
            if raise_error:
                raise e
            else:
                logger.error(f"error downloading file from S3 {bucket}/{key}: {e}")
        retries += 1
    return False


def download_transformed_resources(
    collection_dir: str,
    bucket: str,
    collection_name: str,
    transformed_dir: str = "transformed/",
    issue_dir: str = "issue/",
    column_field_dir: str = "var/column-field/",
    dataset_resource_dir: str = "var/dataset-resource/",
    converted_resource_dir: str = "var/converted-resource/",
    max_threads: int = 4,
    transformation_offset: int = None,
    transformation_limit: int = None
) -> None:
    """Download transformed resources and related files from S3.

    Args:
        collection_dir: Local collection directory
        bucket: S3 bucket name
        collection_name: Collection name (e.g., 'brownfield-land')
        transformed_dir: Local directory for transformed files
        issue_dir: Local directory for issue files
        column_field_dir: Local directory for column field mappings
        dataset_resource_dir: Local directory for dataset resource mappings
        converted_resource_dir: Local directory for converted resources
        max_threads: Maximum concurrent downloads
        transformation_offset: Optional offset for filtering resources
        transformation_limit: Optional limit for filtering resources
    """
    if not HAS_BOTO3:
        raise ImportError("boto3 is required. Install it with: pip install boto3")

    # Load collection to get resource list
    collection = Collection(name=None, directory=collection_dir)
    collection.load()

    # Get repository name (collection name without -collection suffix)
    repository = collection_name.rstrip('-collection')

    # Build resource list with offset/limit
    dataset_resource_map = collection.dataset_resource_map()

    sorted_resource_list = []
    for key in sorted(dataset_resource_map.keys()):
        sorted_resource_list.extend(sorted(dataset_resource_map[key]))

    filtered_resources = sorted_resource_list
    if transformation_offset is not None:
        filtered_resources = filtered_resources[transformation_offset:]
    if transformation_limit is not None:
        filtered_resources = filtered_resources[:transformation_limit]

    resources_to_download = list(set(filtered_resources))

    # Build download map for all files we need
    s3_client = boto3.client('s3')
    download_tasks = []

    # Add transformed files
    for resource in resources_to_download:
        s3_key = f"{repository}/{transformed_dir}{resource}.csv"
        local_path = f"{transformed_dir}{resource}.csv"
        download_tasks.append((s3_key, local_path))

    # Download transformed files with progress bar
    logger.info(f"Downloading {len(download_tasks)} transformed files from S3...")

    use_progress_bar = sys.stdout.isatty()

    def download_task(task):
        s3_key, local_path = task
        return download_s3_file(s3_client, bucket, s3_key, local_path)

    with ThreadPoolExecutor(max_threads) as executor:
        if use_progress_bar:
            results = list(tqdm(
                executor.map(download_task, download_tasks),
                total=len(download_tasks),
                desc="Downloading transformed files"
            ))
        else:
            results = []
            futures = list(executor.map(download_task, download_tasks))
            for i, result in enumerate(futures, 1):
                results.append(result)
                if i % 10 == 0:
                    logger.info(f"Downloaded {i}/{len(download_tasks)} files")
            logger.info(f"Completed download of {len(download_tasks)} files")

    # Download supporting directories (issue, column-field, dataset-resource, converted-resource)
    logger.info("Downloading supporting files (issue, column-field, dataset-resource, converted-resource)...")

    # Use aws s3 sync for directory downloads (more efficient for many files)
    import subprocess

    dirs_to_sync = [
        (f"{repository}/{issue_dir}", issue_dir),
        (f"{repository}/{column_field_dir}", column_field_dir),
        (f"{repository}/{dataset_resource_dir}", dataset_resource_dir),
        (f"{repository}/{converted_resource_dir}", converted_resource_dir),
    ]

    for s3_path, local_path in dirs_to_sync:
        logger.info(f"Syncing {s3_path}...")
        try:
            subprocess.run([
                "aws", "s3", "sync",
                f"s3://{bucket}/{s3_path}",
                local_path,
                "--no-progress"
            ], check=True)
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to sync {s3_path}: {e}")
            raise

    logger.info("Download complete!")


@click.command()
@click.option(
    "--collection-dir",
    required=True,
    type=click.Path(exists=True),
    help="Path to the collection directory"
)
@click.option(
    "--bucket",
    required=True,
    help="S3 bucket name to download from"
)
@click.option(
    "--collection-name",
    default=None,
    help="Collection name (e.g., 'brownfield-land'). If not provided, will use COLLECTION_NAME env var."
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
    collection_name,
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
    """Download transformed resources and supporting files from S3."""
    # Configure logging
    if verbose:
        logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
    else:
        logging.basicConfig(level=logging.WARNING, format='%(levelname)s: %(message)s')

    # Get collection name from parameter or environment variable
    import os
    if not collection_name:
        collection_name = os.environ.get('COLLECTION_NAME')
        if not collection_name:
            click.echo("Error: --collection-name must be provided or COLLECTION_NAME environment variable must be set", err=True)
            sys.exit(1)

    try:
        download_transformed_resources(
            collection_dir=collection_dir,
            bucket=bucket,
            collection_name=collection_name,
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
    except Exception as e:
        click.echo(f"\nDownload failed: {e}", err=True)
        sys.exit(1)


if __name__ == "__main__":
    run_command()
