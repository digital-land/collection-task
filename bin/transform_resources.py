import logging
import sys
import click

from collection_task.transform import process_resources

logger = logging.getLogger(__name__)


@click.command()
@click.option(
    "--collection-dir",
    required=True,
    type=click.Path(exists=True),
    help="Path to the collection directory"
)
@click.option(
    "--pipeline-dir",
    default="pipeline/",
    help="Path to the pipeline configuration directory"
)
@click.option(
    "--cache-dir",
    default="var/cache/",
    help="Path to the cache directory"
)
@click.option(
    "--transformed-dir",
    default="transformed/",
    help="Path to the transformed output directory"
)
@click.option(
    "--issue-dir",
    default="issue/",
    help="Path to the issue directory"
)
@click.option(
    "--operational-issue-dir",
    default="performance/operational_issue/",
    help="Path to the operational issue directory"
)
@click.option(
    "--output-log-dir",
    default="log/",
    help="Path to the output log directory"
)
@click.option(
    "--column-field-dir",
    default="var/column-field/",
    help="Path to the column field directory"
)
@click.option(
    "--dataset-resource-dir",
    default="var/dataset-resource/",
    help="Path to the dataset resource directory"
)
@click.option(
    "--converted-resource-dir",
    default="var/converted-resource/",
    help="Path to the converted resource directory"
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
    "--max-workers",
    default=None,
    type=int,
    help="Number of worker processes (defaults to CPU count)"
)
@click.option(
    "--specification-dir",
    default="specification/",
    help="Path to the specification directory"
)
@click.option(
    "--state-path",
    default=None,
    type=click.Path(exists=True),
    help="Path to state.json for stable ordered resource list"
)
@click.option(
    "--reprocess",
    is_flag=True,
    default=False,
    help="Reprocess all resources, even those whose dataset resource log is already up-to-date"
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
def run_command(
    collection_dir,
    pipeline_dir,
    specification_dir,
    cache_dir,
    transformed_dir,
    issue_dir,
    operational_issue_dir,
    output_log_dir,
    column_field_dir,
    dataset_resource_dir,
    converted_resource_dir,
    dataset,
    offset,
    limit,
    max_workers,
    state_path,
    reprocess,
    quiet,
    debug
):
    """Process resources using multiprocessing instead of make."""
    if debug:
        logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s: %(message)s')
    elif quiet:
        logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s: %(message)s')
    else:
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')

    try:
        result = process_resources(
            collection_dir=collection_dir,
            pipeline_dir=pipeline_dir,
            specification_dir=specification_dir,
            cache_dir=cache_dir,
            transformed_dir=transformed_dir,
            issue_dir=issue_dir,
            operational_issue_dir=operational_issue_dir,
            output_log_dir=output_log_dir,
            column_field_dir=column_field_dir,
            dataset_resource_dir=dataset_resource_dir,
            converted_resource_dir=converted_resource_dir,
            dataset=dataset,
            offset=offset,
            limit=limit,
            max_workers=max_workers,
            state_path=state_path,
            reprocess=reprocess,
        )

        if result is None:
            click.echo("\nNo transformation tasks to process")
            sys.exit(0)

        successful, failed, errors = result

        click.echo(f"\nProcessing complete!")
        click.echo(f"Successful: {successful}")
        click.echo(f"Failed: {failed}")

        if failed > 0:
            sys.exit(1)
    except ValueError as e:
        click.echo(f"\nError: {e}", err=True)
        sys.exit(1)


if __name__ == "__main__":
    run_command()
