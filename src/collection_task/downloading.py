"""Downloading functions for collection tasks."""

import logging
import sys
from pathlib import Path
from urllib.request import urlretrieve
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm

try:
    import boto3
    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO3 = False

logger = logging.getLogger(__name__)


def download_file(url, output_path, raise_error=False, max_retries=5):
    """Downloads a file from an S3 or HTTP(S) URL.

    Automatically detects S3 URLs (s3://) and uses boto3 client.
    For HTTP(S) URLs, uses standard urlretrieve.

    Args:
        url: S3 URL (s3://bucket/key) or HTTP(S) URL
        output_path: Local path to save the file
        raise_error: Whether to raise exceptions or log them
        max_retries: Maximum number of retry attempts

    Returns:
        True if download succeeded, False otherwise
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Check if this is an s3:// URL
    if url.startswith('s3://'):
        if not HAS_BOTO3:
            error_msg = "boto3 is required to download from s3:// URLs. Install it with: pip install boto3"
            logger.error(error_msg)
            if raise_error:
                raise ImportError(error_msg)
            return False

        parsed = urlparse(url)
        bucket = parsed.netloc
        key = parsed.path.lstrip('/')

        retries = 0
        while retries < max_retries:
            try:
                s3 = boto3.client('s3')
                s3.download_file(bucket, key, str(output_path))
                return True
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
                urlretrieve(url, str(output_path))
                return True
            except Exception as e:
                if raise_error:
                    raise e
                else:
                    logger.error(f"error downloading file from url {url}: {e}")
                retries += 1

    return False


def download_files(url_map, max_threads=4):
    """Downloads multiple files concurrently using threads.

    Args:
        url_map: Dictionary mapping URLs to local output paths {url: output_path}
        max_threads: Maximum number of concurrent download threads

    Returns:
        List of boolean results indicating success/failure for each download

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
        total_files = len(futures)

        # Use tqdm for interactive terminals, plain iteration for cloud/non-interactive
        if use_progress_bar:
            iterator = tqdm(futures, desc="Downloading files")
        else:
            iterator = futures
            logger.info(f"Starting download of {total_files} files...")

        last_logged_percent = 0

        for i, future in enumerate(iterator, 1):
            url = futures[future]
            try:
                result = future.result()
                results.append(result)

                # Track failed downloads
                if not result:
                    failed_downloads.append(url)

                # Log progress at 10% intervals in non-interactive mode
                if not use_progress_bar:
                    current_percent = (i * 100) // total_files
                    if current_percent >= last_logged_percent + 10 or i == total_files:
                        logger.info(f"Progress: {i}/{total_files} files ({current_percent}%)")
                        last_logged_percent = current_percent
            except Exception as e:
                error_msg = f"Failed to download {url}: {e}"
                logger.error(error_msg)
                failed_downloads.append(url)

        if not use_progress_bar:
            logger.info(f"Completed download of {total_files} files")

        # Raise an error if any downloads failed
        if failed_downloads:
            error_summary = f"Failed to download {len(failed_downloads)} file(s):\n" + "\n".join(failed_downloads)
            logger.error(error_summary)
            raise RuntimeError(error_summary)

        return results
