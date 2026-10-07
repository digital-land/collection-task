"""Acceptance tests for bin/retire_dataset_files.py and bin/retire.sh

boto3.client is replaced with a fake S3 client, so no AWS access is required.
"""

import os
import subprocess
from pathlib import Path

import pytest
from click.testing import CliRunner

import retire_dataset_files

RETIRE_SH = Path(__file__).parent.parent.parent / "bin" / "retire.sh"
BUCKET = "development-collection-data"
PLANNING_APPLICATION_FILES = [
    "dataset/planning-application.csv",
    "planning-application-collection/dataset/planning-application.sqlite3",
]
OTHER_FILES = [
    "dataset/planning-application-type.csv",
    "planning-application-collection/collection/resource.csv",
]


class FakeS3Client:
    """Just enough of a boto3 S3 client for the command: a single-page listing and delete_objects"""

    def __init__(self, keys):
        self.keys = set(keys)
        self.delete_calls = []

    def get_paginator(self, operation_name):
        return self

    def paginate(self, Bucket, Prefix):
        yield {"Contents": [{"Key": key} for key in sorted(self.keys) if key.startswith(Prefix)]}

    def delete_objects(self, Bucket, Delete):
        self.delete_calls.append(Delete)
        for item in Delete["Objects"]:
            self.keys.discard(item["Key"])
        return {}


@pytest.fixture()
def client(mocker):
    client = FakeS3Client(PLANNING_APPLICATION_FILES + OTHER_FILES)
    mocker.patch("retire_dataset_files.boto3.client", return_value=client)
    return client


def _run(*extra_args):
    return CliRunner().invoke(
        retire_dataset_files.main,
        ["--bucket", BUCKET, "--dataset", "planning-application", "--collection", "planning-application", *extra_args],
    )


def test_command_is_a_dry_run_by_default(client):
    """Should list the files and remove nothing when --dry-run is not given"""
    result = _run()

    assert result.exit_code == 0, result.output
    assert client.delete_calls == []
    assert client.keys == set(PLANNING_APPLICATION_FILES + OTHER_FILES)


def test_command_removes_the_dataset_files_when_dry_run_is_false(client):
    """Should remove the dataset's files, and only those, with --dry-run false"""
    result = _run("--dry-run", "false")

    assert result.exit_code == 0, result.output
    assert client.keys == set(OTHER_FILES)


@pytest.mark.parametrize("missing", ["COLLECTION_NAME", "DATASET_NAME", "COLLECTION_DATASET_BUCKET_NAME"])
def test_retire_sh_fails_without_each_required_variable(missing):
    """Should exit with an error before doing anything if a required variable is missing"""
    env = {
        **os.environ,
        "COLLECTION_NAME": "planning-application",
        "DATASET_NAME": "planning-application",
        "COLLECTION_DATASET_BUCKET_NAME": BUCKET,
    }
    del env[missing]

    result = subprocess.run(["bash", str(RETIRE_SH)], env=env, capture_output=True, text=True)

    assert result.returncode == 1
    assert f"Error: {missing} environment variable must be set" in result.stdout
