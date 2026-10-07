"""Removing the user-facing files of a dataset that is no longer built in an environment."""

import logging

logger = logging.getLogger(__name__)

# S3's DeleteObjects accepts at most 1000 keys per request
DELETE_BATCH_SIZE = 1000


def dataset_file_prefixes(dataset, collection):
    """The prefixes of the files a dataset publishes: the public downloads under dataset/ and the
    built files in its collection's dataset/ folder. The trailing dot stops one dataset matching
    another whose name starts with it, e.g. tree and tree-preservation-order."""
    return [f"dataset/{dataset}.", f"{collection}-collection/dataset/{dataset}."]


def list_files(s3_client, bucket, prefixes):
    """Every key in the bucket under any of the prefixes."""
    paginator = s3_client.get_paginator("list_objects_v2")
    keys = []
    for prefix in prefixes:
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            keys.extend(item["Key"] for item in page.get("Contents", []))
    return keys


def remove_files(s3_client, bucket, keys):
    """Delete the keys in batches, failing if S3 reports any key it could not delete."""
    for start in range(0, len(keys), DELETE_BATCH_SIZE):
        batch = keys[start : start + DELETE_BATCH_SIZE]
        response = s3_client.delete_objects(
            Bucket=bucket,
            Delete={"Objects": [{"Key": key} for key in batch], "Quiet": True},
        )
        errors = response.get("Errors", [])
        if errors:
            raise RuntimeError(f"failed to delete {len(errors)} file(s) from {bucket}: {errors}")


def is_dry_run(value):
    """Only an explicit "false" deletes anything, so a missing or mistyped value just lists."""
    return (value or "").strip().lower() != "false"


def retire_dataset_files(s3_client, bucket, dataset, collection, dry_run=True):
    """List the dataset's user-facing files and, unless this is a dry run, delete them.

    The bucket is versioned, so a deleted file can be restored for a few days afterwards."""
    keys = list_files(s3_client, bucket, dataset_file_prefixes(dataset, collection))

    if not keys:
        logger.info(f"no files found for {dataset} in {bucket}")
        return keys

    action = "would remove" if dry_run else "removing"
    for key in keys:
        logger.info(f"{action} s3://{bucket}/{key}")

    if not dry_run:
        remove_files(s3_client, bucket, keys)
        logger.info(f"removed {len(keys)} file(s) for {dataset} from {bucket}")

    return keys
