"""Unit tests for collection_task.retire"""

import logging

import pytest

from collection_task.retire import (
    DELETE_BATCH_SIZE,
    dataset_file_prefixes,
    is_dry_run,
    list_files,
    remove_files,
    retire_dataset_files,
)

BUCKET = "development-collection-data"

# The files planning-application left in production, as listed by the retire-dataset DAG on
# 2026-10-07, plus neighbours whose names start with "planning-application" which must survive
PLANNING_APPLICATION_FILES = [
    "dataset/planning-application.csv",
    "dataset/planning-application.geojson",
    "dataset/planning-application.json",
    "planning-application-collection/dataset/planning-application.csv",
    "planning-application-collection/dataset/planning-application.sqlite3",
    "planning-application-collection/dataset/planning-application.sqlite3.json",
]
OTHER_FILES = [
    "dataset/planning-application-type.csv",
    "planning-application-collection/dataset/planning-application-condition.sqlite3",
    "planning-application-collection/collection/resource.csv",
    "planning-application-collection/transformed/planning-application/abc123.parquet",
]


class FakePaginator:
    def __init__(self, client):
        self.client = client

    def paginate(self, Bucket, Prefix):
        assert Bucket == self.client.bucket
        keys = sorted(key for key in self.client.keys if key.startswith(Prefix))
        if not keys:
            # S3 leaves Contents out of an empty listing altogether
            yield {"KeyCount": 0}
            return
        for start in range(0, len(keys), self.client.page_size):
            yield {"Contents": [{"Key": key} for key in keys[start : start + self.client.page_size]]}


class FakeS3Client:
    """Just enough of a boto3 S3 client: list_objects_v2 pagination and delete_objects"""

    def __init__(self, keys, bucket=BUCKET, page_size=1000, errors=None):
        self.keys = set(keys)
        self.bucket = bucket
        self.page_size = page_size
        self.errors = errors or []
        self.delete_calls = []

    def get_paginator(self, operation_name):
        assert operation_name == "list_objects_v2"
        return FakePaginator(self)

    def delete_objects(self, Bucket, Delete):
        assert Bucket == self.bucket
        self.delete_calls.append(Delete)
        for item in Delete["Objects"]:
            self.keys.discard(item["Key"])
        return {"Errors": self.errors} if self.errors else {}


# Test dataset_file_prefixes

def test_dataset_file_prefixes_covers_public_downloads_and_collection_dataset_folder():
    """Should cover dataset/ and the collection's dataset/ folder, ending each with a dot"""
    assert dataset_file_prefixes("planning-application", "planning-application") == [
        "dataset/planning-application.",
        "planning-application-collection/dataset/planning-application.",
    ]


# Test list_files

def test_list_files_finds_only_the_dataset_files():
    """Should list the dataset's files but not those of datasets whose names start with it"""
    client = FakeS3Client(PLANNING_APPLICATION_FILES + OTHER_FILES)

    keys = list_files(client, BUCKET, dataset_file_prefixes("planning-application", "planning-application"))

    assert sorted(keys) == sorted(PLANNING_APPLICATION_FILES)


def test_list_files_follows_every_page():
    """Should not stop at the first page of a listing"""
    keys = [f"dataset/tree.part-{i:04}.csv" for i in range(25)]
    client = FakeS3Client(keys, page_size=10)

    assert sorted(list_files(client, BUCKET, ["dataset/tree."])) == sorted(keys)


def test_list_files_returns_nothing_when_no_files_match():
    """Should handle an empty listing, which has no Contents"""
    client = FakeS3Client(OTHER_FILES)

    assert list_files(client, BUCKET, ["dataset/planning-application."]) == []


# Test remove_files

def test_remove_files_deletes_in_batches_s3_accepts():
    """Should split the keys into requests of at most DELETE_BATCH_SIZE"""
    keys = [f"dataset/tree.part-{i:05}.csv" for i in range(2 * DELETE_BATCH_SIZE + 500)]
    client = FakeS3Client(keys)

    remove_files(client, BUCKET, keys)

    assert [len(call["Objects"]) for call in client.delete_calls] == [DELETE_BATCH_SIZE, DELETE_BATCH_SIZE, 500]
    assert client.keys == set()


def test_remove_files_raises_when_s3_reports_errors():
    """Should fail rather than carry on when S3 could not delete some keys"""
    client = FakeS3Client(
        PLANNING_APPLICATION_FILES,
        errors=[{"Key": "dataset/planning-application.csv", "Code": "AccessDenied", "Message": "Access Denied"}],
    )

    with pytest.raises(RuntimeError, match="failed to delete 1 file"):
        remove_files(client, BUCKET, PLANNING_APPLICATION_FILES)


def test_remove_files_does_nothing_for_no_keys():
    """Should not send an empty delete request"""
    client = FakeS3Client(OTHER_FILES)

    remove_files(client, BUCKET, [])

    assert client.delete_calls == []


# Test is_dry_run

@pytest.mark.parametrize("value", [None, "", "true", "True", "yes", "flase", "0"])
def test_is_dry_run_for_anything_but_false(value):
    """Should only list when DRY_RUN is missing, empty or anything other than false"""
    assert is_dry_run(value) is True


@pytest.mark.parametrize("value", ["false", "False", "FALSE", " false "])
def test_is_dry_run_is_off_only_for_false(value):
    """Should remove files only when DRY_RUN is explicitly false"""
    assert is_dry_run(value) is False


# Test retire_dataset_files

def test_retire_dataset_files_dry_run_lists_without_deleting(caplog):
    """Should log what it would remove and leave every file in place"""
    client = FakeS3Client(PLANNING_APPLICATION_FILES + OTHER_FILES)

    with caplog.at_level(logging.INFO):
        keys = retire_dataset_files(client, BUCKET, "planning-application", "planning-application", dry_run=True)

    assert sorted(keys) == sorted(PLANNING_APPLICATION_FILES)
    assert client.delete_calls == []
    assert client.keys == set(PLANNING_APPLICATION_FILES + OTHER_FILES)
    assert f"would remove s3://{BUCKET}/dataset/planning-application.csv" in caplog.text


def test_retire_dataset_files_is_a_dry_run_by_default():
    """Should not delete anything unless told to"""
    client = FakeS3Client(PLANNING_APPLICATION_FILES)

    retire_dataset_files(client, BUCKET, "planning-application", "planning-application")

    assert client.delete_calls == []


def test_retire_dataset_files_removes_only_the_dataset_files():
    """Should delete the dataset's files and leave its neighbours alone"""
    client = FakeS3Client(PLANNING_APPLICATION_FILES + OTHER_FILES)

    keys = retire_dataset_files(client, BUCKET, "planning-application", "planning-application", dry_run=False)

    assert sorted(keys) == sorted(PLANNING_APPLICATION_FILES)
    assert client.keys == set(OTHER_FILES)


def test_retire_dataset_files_does_nothing_when_no_files_are_found(caplog):
    """Should log that nothing was found and send no delete request"""
    client = FakeS3Client(OTHER_FILES)

    with caplog.at_level(logging.INFO):
        keys = retire_dataset_files(client, BUCKET, "planning-application", "planning-application", dry_run=False)

    assert keys == []
    assert client.delete_calls == []
    assert f"no files found for planning-application in {BUCKET}" in caplog.text
