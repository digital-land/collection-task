import logging
import sqlite3
import click
import duckdb
from pathlib import Path
from cloudpathlib import AnyPath, S3Path

from digital_land.organisation import Organisation
from digital_land.package.dataset import DatasetPackage
from digital_land.specification import Specification

logger = logging.getLogger(__name__)

# Known columns present in each Hive-partitioned parquet table.
# fact, fact_resource and issue do not have start_date or end_date.
PARQUET_COLUMNS = {
    "entity": [
        "dataset", "end_date", "entity", "entry_date", "geometry",
        "json", "name", "organisation_entity", "point", "prefix", "quality",
        "reference", "start_date", "typology",
    ],
    "fact": [
        "entity", "fact", "field", "entry_date", "priority", "reference_entity", "value",
    ],
    "fact_resource": [
        "fact", "entry_date", "entry_number", "priority", "resource",
    ],
    "issue": [
        "entity", "entry_date", "entry_number", "field", "issue_type",
        "line_number", "dataset", "resource", "value", "message",
    ],
}

# CSV-sourced tables — schema still read dynamically from SQLite after create_database()
CSV_SQLITE_TABLES = ["dataset_resource", "column_field", "old_entity"]


def _csv_sources(collection_data_path, collection, dataset, entity_min, entity_max):
    """Return (sqlite_table, path, where_clause) tuples for tables loaded from CSV."""
    base = f"{collection_data_path}/{collection}-collection"
    return [
        ("dataset_resource", f"{base}/var/dataset-resource/{dataset}/*.csv", None),
        ("column_field", f"{base}/var/column-field/{dataset}/*.csv", None),
        (
            "old_entity",
            f"{collection_data_path}/config/pipeline/{collection}/old-entity.csv",
            f'CAST("old-entity" AS INTEGER) BETWEEN {entity_min} AND {entity_max}',
        ),
    ]


def _load_csv_table(conn, sqlite_table, path, table_columns, where_clause=None):
    """Load a CSV table into the attached SQLite database via DuckDB."""
    # Check the parent directory exists before attempting the load
    parent_path = AnyPath(path.rsplit("/", 1)[0])
    if not parent_path.exists():
        logger.debug(f"No directory at {parent_path}, skipping '{sqlite_table}'")
        return

    csv_cols = {
        row[0]
        for row in conn.execute(
            f"DESCRIBE SELECT * FROM read_csv_auto('{path}') LIMIT 0"
        ).fetchall()
    }
    cols = [c for c in table_columns[sqlite_table] if c in csv_cols]
    if not cols:
        logger.debug(f"No matching columns for '{sqlite_table}', skipping")
        return

    # SQLite column names use underscores (via colname()), but CSV headers use
    # hyphens — alias each column so DuckDB maps the CSV header to the SQLite name
    sqlite_col_list = ", ".join(f'"{c}"' for c in cols)
    csv_col_list = ", ".join(f'"{c.replace("_", "-")}" AS "{c}"' for c in cols)
    where = f"WHERE {where_clause}" if where_clause else ""
    logger.info(f"Loading CSV file(s) into '{sqlite_table}'")
    conn.execute(f"""
        INSERT INTO sqlite_db."{sqlite_table}" ({sqlite_col_list})
        SELECT {csv_col_list}
        FROM read_csv_auto('{path}')
        {where}
    """)


def build_dataset_package(
    dataset,
    parquet_datasets_path,
    output_path,
    specification_dir,
    collection_data_path=None,
    collection=None,
):
    """Build a dataset SQLite file from pre-built parquet tables and CSV files.

    Args:
        dataset: Dataset name
        parquet_datasets_path: Base path containing parquet table subdirectories (s3:// or local)
        output_path: Path for the output SQLite file
        specification_dir: Path to the specification directory
        collection_data_path: Base path to the collection data (s3://bucket or local path, optional)
        collection: Collection name used to build CSV paths (optional)
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    base_path = AnyPath(parquet_datasets_path)

    # Load specification to get entity range for old-entity filtering
    spec = Specification(specification_dir)
    entity_min = spec.get_dataset_entity_min(dataset)
    entity_max = spec.get_dataset_entity_max(dataset)

    # Create the SQLite schema
    logger.info(f"Creating SQLite schema at {output_path}")
    package = DatasetPackage(
        dataset,
        organisation=Organisation(organisation={}),
        path=str(output_path),
        specification_dir=specification_dir,
    )
    package.create_database()
    package.disconnect()

    # Read the SQLite schema for CSV tables only
    db_conn = sqlite3.connect(str(output_path))
    csv_table_columns = {
        t: [row[1] for row in db_conn.execute(f'PRAGMA table_info("{t}")').fetchall()]
        for t in CSV_SQLITE_TABLES
    }
    db_conn.close()

    # Use DuckDB to load all tables
    logger.info(f"Loading tables from {base_path} into {output_path}")
    conn = duckdb.connect()
    conn.execute("INSTALL httpfs; LOAD httpfs;")
    conn.execute("INSTALL sqlite; LOAD sqlite;")
    if isinstance(base_path, S3Path) or isinstance(AnyPath(collection_data_path), S3Path):
        conn.execute("CREATE SECRET (TYPE S3, PROVIDER CREDENTIAL_CHAIN);")
    conn.execute(f"ATTACH DATABASE '{output_path}' AS sqlite_db (TYPE SQLITE);")

    # Load parquet tables — scan only the target dataset's partition directory
    # to avoid hive partition schema mismatches across different dataset partitions
    for table_name, cols in PARQUET_COLUMNS.items():
        dataset_partition_path = base_path / table_name / f"dataset={dataset}"

        if not dataset_partition_path.exists():
            logger.debug(f"No directory at {dataset_partition_path}, skipping '{table_name}'")
            continue

        scan_path = f"{dataset_partition_path}/**/*.parquet"
        # dataset is a virtual hive column — supply it as a literal in the same
        # position to keep col_list and select_list aligned
        col_list = ", ".join(f'"{c}"' for c in cols)
        select_list = ", ".join(
            f"'{dataset}' AS \"dataset\"" if c == "dataset" else f'"{c}"'
            for c in cols
        )

        logger.info(f"Loading parquet files into '{table_name}'")
        conn.execute(f"""
            INSERT INTO sqlite_db."{table_name}" ({col_list})
            SELECT {select_list}
            FROM parquet_scan('{scan_path}')
        """)

    # Load CSV tables from the collection data path
    if collection_data_path and collection:
        logger.info(f"Loading CSV tables from {collection_data_path}")
        for sqlite_table, path, where_clause in _csv_sources(
            collection_data_path, collection, dataset, entity_min, entity_max
        ):
            _load_csv_table(conn, sqlite_table, path, csv_table_columns, where_clause)
    else:
        logger.debug("No collection data path configured, skipping CSV tables")

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
    "--parquet-datasets-path",
    required=True,
    help="Base path containing the parquet table directories (s3://bucket/prefix or local path)",
)
@click.option("--output-path", required=True, help="Path for the output SQLite file")
@click.option(
    "--specification-dir",
    default="specification/",
    help="Path to the specification directory",
)
@click.option(
    "--collection-data-path",
    required=True,
    help="Base path to collection data for CSV tables (s3://bucket or local path)",
)
@click.option(
    "--collection",
    required=True,
    help="Collection name used to build CSV paths (e.g. brownfield-land)",
)
@click.option("--quiet", is_flag=True, help="Suppress progress output")
@click.option("--debug", is_flag=True, help="Enable debug logging")
def run_command(
    dataset,
    parquet_datasets_path,
    output_path,
    specification_dir,
    collection_data_path,
    collection,
    quiet,
    debug,
):
    """Build a dataset SQLite package from pre-built parquet tables and collection CSVs.

    Parquet tables (entity, fact, fact-resource, issue) are read from --parquet-datasets-path.
    CSV tables (dataset-resource, column-field, old-entity) are read from
    --collection-data-path.

    Example:
        python bin/build_dataset_package.py \\
            --dataset central-activities-zone \\
            --parquet-datasets-path s3://parquet-datasets-bucket/central-activities-zone \\
            --collection-data-path s3://collection-data-bucket \\
            --collection central-activities-zone \\
            --output-path dataset/central-activities-zone.sqlite3
    """
    if debug:
        logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s: %(message)s")
    elif quiet:
        logging.basicConfig(level=logging.WARNING, format="%(levelname)s: %(message)s")
    else:
        logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    build_dataset_package(
        dataset,
        parquet_datasets_path,
        output_path,
        specification_dir,
        collection_data_path=collection_data_path,
        collection=collection,
    )


if __name__ == "__main__":
    run_command()
