"""Integration tests for collection_task.transform"""

import pytest
from collection_task.transform import process_single_resource


def _make_args(resource_path, old_resource="resource-hash-abc", dataset="some-dataset", **config_overrides):
    config = {
        "pipeline_dir": "pipeline/",
        "cache_dir": "var/cache/",
        "collection_dir": "collection/",
        "transformed_dir": "transformed/",
        "issue_dir": "issue/",
        "operational_issue_dir": "performance/operational_issue/",
        "output_log_dir": "log/",
        "column_field_dir": "var/column-field/",
        "dataset_resource_dir": "var/dataset-resource/",
        "converted_resource_dir": "var/converted-resource/",
        "config_path": "var/cache/config.sqlite3",
        "organisation_path": "var/cache/organisation.csv",
        **config_overrides,
    }
    return (old_resource, dataset, resource_path, "", "", None, config)


def test_process_single_resource_raises_if_resource_file_missing(tmp_path):
    """Should raise FileNotFoundError when the resource file does not exist."""
    missing_path = tmp_path / "does-not-exist"

    with pytest.raises(FileNotFoundError, match="resource-hash-abc"):
        process_single_resource(_make_args(missing_path))


def test_process_single_resource_error_includes_dataset_name(tmp_path):
    """FileNotFoundError message should include the dataset name for easier debugging."""
    missing_path = tmp_path / "does-not-exist"

    with pytest.raises(FileNotFoundError, match="some-dataset"):
        process_single_resource(_make_args(missing_path))


def test_process_single_resource_error_includes_path(tmp_path):
    """FileNotFoundError message should include the expected path."""
    missing_path = tmp_path / "does-not-exist"

    with pytest.raises(FileNotFoundError, match=str(missing_path)):
        process_single_resource(_make_args(missing_path))
