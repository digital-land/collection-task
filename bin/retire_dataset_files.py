import logging

import boto3
import click

from collection_task.retire import is_dry_run, retire_dataset_files


@click.command()
@click.option("--bucket", required=True, help="The collection data bucket")
@click.option("--dataset", required=True, help="The dataset whose files to remove")
@click.option("--collection", required=True, help="The dataset's collection, without the -collection suffix")
@click.option("--dry-run", "dry_run", default="true", help='Only "false" removes anything; any other value just lists the files')
def main(bucket, dataset, collection, dry_run):
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    retire_dataset_files(boto3.client("s3"), bucket, dataset, collection, dry_run=is_dry_run(dry_run))


if __name__ == "__main__":
    main()
