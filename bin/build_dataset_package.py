import logging
import sqlite3
import sys
import click
import duckdb
from pathlib import Path
from cloudpathlib import AnyPath

from digital_land.package.dataset import DatasetPackage

logger = logging.getLogger(__name__)

# Maps parquet directory names (underscores) to SQLite table names (hyphens)
TABLE_MAP = {
    "entity": "entity",
    "fact": "fact",
    "fact_resource": "fact-resource",
    "issue": "issue",
    "dataset_resource": "dataset-resource",
    "column_field": "column-field",
    "old_entity": "old-entity",
}


def build_dataset_package(dataset, parquet_path, output_path, specification_dir):
    """Build a dataset SQLite file from pre-built parquet tables.

    Args:
        dataset: Dataset name
        parquet_path: Base path containing table subdirectories (s3:// or local)
        output_path: Path for the output SQLite file
        specification_dir: Path to the specification directory
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    base_path = AnyPath(parquet_path)

    # Create the SQLite schema
    logger.info(f"Creating SQLite schema at {output_path}")
    package = DatasetPackage(
        dataset,
        organisation=None,
        path=str(output_path),
        specification_dir=specification_dir,
    )
    package.create_database()
    package.disconnect()

    # Read the SQLite schema so we can SELECT only the columns that belong in each table
    db_conn = sqlite3.connect(str(output_path))
    table_columns = {}
    for sqlite_table in TABLE_MAP.values():
        cols = [row[1] for row in db_conn.execute(f'PRAGMA table_info("{sqlite_table}")').fetchall()]
        table_columns[sqlite_table] = cols
    db_conn.close()

    # Use DuckDB to load each parquet table directly from the path (S3 or local)
    logger.info(f"Loading parquet tables from {base_path} into {output_path}")
    conn = duckdb.connect()
    conn.execute("INSTALL httpfs; LOAD httpfs;")
    conn.execute("INSTALL sqlite; LOAD sqlite;")
    conn.execute(f"ATTACH DATABASE '{output_path}' AS sqlite_db (TYPE SQLITE);")

    for table_name, sqlite_table in TABLE_MAP.items():
        table_path = base_path / table_name
        parquet_files = list(table_path.glob("**/*.parquet"))

        if not parquet_files:
            logger.debug(f"No parquet files at {table_path}, skipping '{sqlite_table}'")
            continue

        cols = table_columns[sqlite_table]
        col_list = ", ".join(f'"{c}"' for c in cols)
        scan_path = f"{table_path}/**/*.parquet"
        logger.info(f"Loading {len(parquet_files)} file(s) into '{sqlite_table}'")
        conn.execute(f"""
            INSERT INTO sqlite_db."{sqlite_table}"
            SELECT {col_list}
            FROM parquet_scan('{scan_path}', hive_partitioning=true)
            WHERE dataset = '{dataset}'
        """)

    conn.execute("DETACH DATABASE sqlite_db;")
    conn.close()

    # Add indexes and counts
    logger.info("Creating indexes")
    package.connect()
    package.create_cursor()
    package.create_indexes()
    package.disconnect()

    logger.info("Adding counts")
    package.add_counts()

    logger.info(f"Dataset package built: {output_path}")


@click.command()
@click.option("--dataset", required=True, help="Dataset name (e.g. central-activities-zone)")
@click.option(
    "--parquet-path",
    required=True,
    help="Base path containing the parquet table directories (s3://bucket/prefix or local path)",
)
@click.option("--output-path", required=True, help="Path for the output SQLite file")
@click.option(
    "--specification-dir",
    default="specification/",
    help="Path to the specification directory",
)
@click.option("--quiet", is_flag=True, help="Suppress progress output")
@click.option("--debug", is_flag=True, help="Enable debug logging")
def run_command(dataset, parquet_path, output_path, specification_dir, quiet, debug):
    """Build a dataset SQLite package from pre-built parquet tables.

    Reads entity/, fact/, fact_resource/, issue/, dataset_resource/,
    column_field/ and old_entity/ parquet tables from the given base path
    (S3 or local) and loads them into a dataset SQLite file.

    Example using S3:
        python bin/build_dataset_package.py \\
            --dataset central-activities-zone \\
            --parquet-path s3://my-bucket/central-activities-zone \\
            --output-path dataset/central-activities-zone.sqlite3

    Example using local path (for testing):
        python bin/build_dataset_package.py \\
            --dataset central-activities-zone \\
            --parquet-path var/parquet/central-activities-zone \\
            --output-path dataset/central-activities-zone.sqlite3
    """
    if debug:
        logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s: %(message)s")
    elif quiet:
        logging.basicConfig(level=logging.WARNING, format="%(levelname)s: %(message)s")
    else:
        logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    try:
        build_dataset_package(dataset, parquet_path, output_path, specification_dir)
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


if __name__ == "__main__":
    run_command()
