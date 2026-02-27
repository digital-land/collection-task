import logging
import sqlite3
import click
import duckdb
from pathlib import Path
from cloudpathlib import AnyPath

from digital_land.organisation import Organisation
from digital_land.package.dataset import DatasetPackage
from digital_land.specification import Specification

logger = logging.getLogger(__name__)

# Tables loaded from Hive-partitioned parquet in the parquet datasets bucket
# Values are the SQLite table names (underscores, from colname() in SqlitePackage)
PARQUET_TABLE_MAP = {
    "entity": "entity",
    "fact": "fact",
    "fact_resource": "fact_resource",
    "issue": "issue",
}

# All SQLite tables — used to read the schema after create_database()
ALL_SQLITE_TABLES = list(PARQUET_TABLE_MAP.values()) + [
    "dataset_resource",
    "column_field",
    "old_entity",
]


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

    # Read the SQLite schema so we can SELECT only the columns that belong in each table
    db_conn = sqlite3.connect(str(output_path))
    table_columns = {}
    for sqlite_table in ALL_SQLITE_TABLES:
        cols = [row[1] for row in db_conn.execute(f'PRAGMA table_info("{sqlite_table}")').fetchall()]
        table_columns[sqlite_table] = cols
    db_conn.close()

    # Use DuckDB to load all tables
    logger.info(f"Loading tables from {base_path} into {output_path}")
    conn = duckdb.connect()
    conn.execute("INSTALL httpfs; LOAD httpfs;")
    conn.execute("INSTALL sqlite; LOAD sqlite;")
    conn.execute("CREATE SECRET (TYPE S3, PROVIDER CREDENTIAL_CHAIN);")
    conn.execute(f"ATTACH DATABASE '{output_path}' AS sqlite_db (TYPE SQLITE);")

    # Load parquet tables (Hive-partitioned by dataset)
    for table_name, sqlite_table in PARQUET_TABLE_MAP.items():
        table_path = base_path / table_name

        if not table_path.exists():
            logger.debug(f"No directory at {table_path}, skipping '{sqlite_table}'")
            continue

        scan_path = f"{table_path}/**/*.parquet"

        # Find which SQLite columns are actually present in the parquet
        # (includes virtual Hive partition columns such as 'dataset')
        parquet_cols = {
            row[0]
            for row in conn.execute(
                f"DESCRIBE SELECT * FROM parquet_scan('{scan_path}', hive_partitioning=true) LIMIT 0"
            ).fetchall()
        }
        cols = [c for c in table_columns[sqlite_table] if c in parquet_cols]
        col_list = ", ".join(f'"{c}"' for c in cols)

        logger.info(f"Loading parquet files into '{sqlite_table}'")
        conn.execute(f"""
            INSERT INTO sqlite_db."{sqlite_table}" ({col_list})
            SELECT {col_list}
            FROM parquet_scan('{scan_path}', hive_partitioning=true)
            WHERE dataset = '{dataset}'
        """)

    # Load CSV tables from the collection data path
    if collection_data_path and collection:
        logger.info(f"Loading CSV tables from {collection_data_path}")
        for sqlite_table, path, where_clause in _csv_sources(
            collection_data_path, collection, dataset, entity_min, entity_max
        ):
            _load_csv_table(conn, sqlite_table, path, table_columns, where_clause)
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
