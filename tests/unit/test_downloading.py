"""Unit tests for collection_task.downloading"""

import logging
import pytest
from pathlib import Path
from unittest.mock import MagicMock
from collection_task.downloading import download_file, download_files


# Test download_file

def test_download_file_creates_parent_directory(tmp_path, mocker):
    """Should create parent directories if they don't exist"""
    output_path = tmp_path / "subdir" / "file.txt"

    mock_urlretrieve = mocker.patch('collection_task.downloading.urlretrieve')
    download_file("https://example.com/file.txt", output_path)

    assert output_path.parent.exists()


def test_download_file_uses_urlretrieve_for_http_urls(tmp_path, mocker):
    """Should use urlretrieve for HTTP URLs"""
    output_path = tmp_path / "file.txt"

    mock_urlretrieve = mocker.patch('collection_task.downloading.urlretrieve')
    result = download_file("https://example.com/file.txt", output_path)

    mock_urlretrieve.assert_called_once_with("https://example.com/file.txt", str(output_path))
    assert result is True


def test_download_file_uses_boto3_for_s3_urls(tmp_path, mocker):
    """Should use boto3 for S3 URLs"""
    output_path = tmp_path / "file.txt"

    mocker.patch('collection_task.downloading.HAS_BOTO3', True)
    mock_s3 = MagicMock()
    mock_boto3_client = mocker.patch('collection_task.downloading.boto3.client', return_value=mock_s3)

    result = download_file("s3://my-bucket/path/file.txt", output_path)

    mock_s3.download_file.assert_called_once_with("my-bucket", "path/file.txt", str(output_path))
    assert result is True


def test_download_file_raises_error_for_s3_without_boto3(tmp_path, mocker):
    """Should raise ImportError for S3 URLs when boto3 not installed"""
    output_path = tmp_path / "file.txt"

    mocker.patch('collection_task.downloading.HAS_BOTO3', False)

    with pytest.raises(ImportError, match="boto3 is required"):
        download_file("s3://my-bucket/file.txt", output_path, raise_error=True)


def test_download_file_logs_error_for_s3_without_boto3_when_not_raising(tmp_path, mocker, caplog):
    """Should log error for S3 URLs when boto3 not installed and raise_error=False"""
    output_path = tmp_path / "file.txt"

    mocker.patch('collection_task.downloading.HAS_BOTO3', False)
    result = download_file("s3://my-bucket/file.txt", output_path, raise_error=False)

    assert result is False
    assert "boto3 is required" in caplog.text


def test_download_file_retries_on_failure(tmp_path, mocker):
    """Should retry up to max_retries times on failure"""
    output_path = tmp_path / "file.txt"

    mock_urlretrieve = mocker.patch('collection_task.downloading.urlretrieve')
    mock_urlretrieve.side_effect = Exception("Network error")

    result = download_file("https://example.com/file.txt", output_path, max_retries=3)

    assert mock_urlretrieve.call_count == 3
    assert result is False


def test_download_file_succeeds_on_retry(tmp_path, mocker):
    """Should succeed if a retry works"""
    output_path = tmp_path / "file.txt"

    mock_urlretrieve = mocker.patch('collection_task.downloading.urlretrieve')
    # Fail twice, then succeed
    mock_urlretrieve.side_effect = [Exception("Error"), Exception("Error"), None]

    result = download_file("https://example.com/file.txt", output_path, max_retries=5)

    assert mock_urlretrieve.call_count == 3
    assert result is True


def test_download_file_raises_error_when_raise_error_true(tmp_path, mocker):
    """Should raise exception when raise_error=True"""
    output_path = tmp_path / "file.txt"

    mock_urlretrieve = mocker.patch('collection_task.downloading.urlretrieve')
    mock_urlretrieve.side_effect = Exception("Network error")

    with pytest.raises(Exception, match="Network error"):
        download_file("https://example.com/file.txt", output_path, raise_error=True)


# Test download_files

def test_download_files_downloads_all_urls(tmp_path, mocker):
    """Should download all URLs in the map"""
    url_map = {
        "https://example.com/file1.txt": tmp_path / "file1.txt",
        "https://example.com/file2.txt": tmp_path / "file2.txt",
    }

    mock_download = mocker.patch('collection_task.downloading.download_file', return_value=True)

    results = download_files(url_map, max_threads=2)

    assert mock_download.call_count == 2
    assert len(results) == 2


def test_download_files_raises_runtime_error_on_failures(tmp_path, mocker):
    """Should raise RuntimeError if any downloads fail"""
    url_map = {
        "https://example.com/file1.txt": tmp_path / "file1.txt",
        "https://example.com/file2.txt": tmp_path / "file2.txt",
    }

    mock_download = mocker.patch('collection_task.downloading.download_file')
    # First succeeds, second fails
    mock_download.side_effect = [True, False]

    with pytest.raises(RuntimeError, match="Failed to download 1 file"):
        download_files(url_map, max_threads=2)


def test_download_files_uses_progress_bar_in_interactive_mode(tmp_path, mocker):
    """Should use tqdm progress bar when in interactive terminal"""
    url_map = {
        "https://example.com/file1.txt": tmp_path / "file1.txt",
    }

    mock_download = mocker.patch('collection_task.downloading.download_file', return_value=True)
    mocker.patch('collection_task.downloading.sys.stdout.isatty', return_value=True)

    # Mock tqdm to pass through the iterable it receives
    mock_tqdm = mocker.patch('collection_task.downloading.tqdm', side_effect=lambda x, **kwargs: x)

    download_files(url_map, max_threads=1)

    # Verify tqdm was called with the futures dict
    mock_tqdm.assert_called_once()
    call_args = mock_tqdm.call_args
    assert call_args.kwargs.get('desc') == "Downloading files"


def test_download_files_logs_progress_in_non_interactive_mode(tmp_path, mocker, caplog):
    """Should log progress when not in interactive terminal"""
    caplog.set_level(logging.INFO)

    url_map = {
        "https://example.com/file1.txt": tmp_path / "file1.txt",
    }

    mock_download = mocker.patch('collection_task.downloading.download_file', return_value=True)
    mocker.patch('collection_task.downloading.sys.stdout.isatty', return_value=False)

    download_files(url_map, max_threads=1)

    assert "Starting download" in caplog.text
    assert "Completed download" in caplog.text
