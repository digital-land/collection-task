"""Acceptance tests for bin/build_dataset_package.py

Uses local Delta tables, partitioned by dataset as pyspark-jobs writes them, created
by the tmp_path fixture so no S3 access is required. The specification is downloaded
once per session using Specification.download().
"""

import sqlite3
from pathlib import Path

import pyarrow as pa
import pytest
from click.testing import CliRunner
from deltalake import DeltaTable, write_deltalake

from build_dataset_package import run_command
from digital_land.organisation import Organisation
from digital_land.package.dataset import DatasetPackage
from digital_land.specification import Specification

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
DATASET = "brownfield-land"
COLLECTION_NAME = "brownfield-land"
OTHER_DATASET = "conservation-area"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write_delta(table_path: Path, dataset: str, table: pa.Table):
    """Write one dataset's rows the way pyspark-jobs' write_delta does: the first write
    creates the table, later writes overwrite only that dataset's partition."""
    table = table.append_column("dataset", pa.array([dataset] * table.num_rows, type=pa.string()))
    if DeltaTable.is_deltatable(str(table_path)):
        write_deltalake(
            str(table_path),
            table,
            partition_by=["dataset"],
            mode="overwrite",
            predicate=f"dataset = '{dataset}'",
        )
    else:
        write_deltalake(str(table_path), table, partition_by=["dataset"])


def _entity_table(entities, names, references) -> pa.Table:
    nulls = pa.array([None] * len(entities), type=pa.string())
    return pa.table({
        "end_date":          nulls,
        "entity":            pa.array(entities, type=pa.int64()),
        "entry_date":        nulls,
        "geojson":           nulls,
        "geometry":          nulls,
        "json":              nulls,
        "name":              pa.array(names, type=pa.string()),
        "organisation_entity": nulls,
        "point":             nulls,
        "prefix":            nulls,
        "quality":           nulls,
        "reference":         pa.array(references, type=pa.string()),
        "start_date":        nulls,
        "typology":          nulls,
    })


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def specification_dir(tmp_path_factory):
    """Download the specification once per test session."""
    path = tmp_path_factory.mktemp("specification")
    Specification.download(path)
    return path


@pytest.fixture()
def parquet_dir(tmp_path):
    """Build minimal Delta tables under tmp_path, partitioned by dataset.

    Two datasets are written so tests can verify the dataset filter works.
    As with pyspark-jobs, dataset is a partition column: it is held in the
    directory name and the Delta log, not as a data column in the parquet files.
    """
    null_str2 = pa.array([None, None], type=pa.string())

    # entity table — two rows for DATASET, one decoy row for OTHER_DATASET
    _write_delta(
        tmp_path / "entity",
        DATASET,
        _entity_table([1, 2], ["Entity One", "Entity Two"], ["ref-1", "ref-2"]),
    )
    _write_delta(
        tmp_path / "entity",
        OTHER_DATASET,
        _entity_table([99], ["Should Not Appear"], ["ref-99"]),
    )

    # fact table — two rows for DATASET only
    _write_delta(
        tmp_path / "fact",
        DATASET,
        pa.table({
            "entity":           pa.array([1, 2], type=pa.int64()),
            "entry_date":       null_str2,
            "fact":             pa.array(["fact-1", "fact-2"], type=pa.string()),
            "field":            pa.array(["name", "reference"], type=pa.string()),
            "priority":         pa.array([None, None], type=pa.int64()),
            "reference_entity": null_str2,
            "value":            pa.array(["Entity One", "ref-1"], type=pa.string()),
        }),
    )

    return tmp_path


@pytest.fixture()
def collection_dir(tmp_path, specification_dir):
    """Build a minimal collection directory tree matching the expected CSV paths.

    Includes one in-range and one out-of-range old-entity row so the filter
    can be verified.
    """
    spec = Specification(str(specification_dir))
    entity_min = int(spec.get_dataset_entity_min(DATASET))
    entity_max = int(spec.get_dataset_entity_max(DATASET))

    base = tmp_path / "collection" / f"{COLLECTION_NAME}-collection"

    # dataset-resource
    dr_dir = base / "var" / "dataset-resource" / DATASET
    dr_dir.mkdir(parents=True)
    (dr_dir / "resource1.csv").write_text(
        "dataset,resource,entity-count,entry-count,line-count,mime-type,code-version\n"
        "brownfield-land,abc123,3,3,4,text/csv,1.0\n"
    )
    # older resources were logged before entity-count and code-version were added to the header
    (dr_dir / "resource2.csv").write_text(
        "dataset,resource,entry-count,line-count,mime-type\n"
        "brownfield-land,def456,5,6,text/csv\n"
    )
    # some older resources were logged with a leading entry-date column
    (dr_dir / "resource3.csv").write_text(
        "entry-date,dataset,resource,entry-count,line-count,mime-type\n"
        "2023-06-19T00:29:55Z,brownfield-land,ghi789,7,8,application/zip\n"
    )

    # column-field
    cf_dir = base / "var" / "column-field" / DATASET
    cf_dir.mkdir(parents=True)
    (cf_dir / "resource1.csv").write_text(
        "dataset,resource,column,field\nbrownfield-land,abc123,Name,name\n"
    )

    # old-entity — one row in range, one outside
    oe_dir = tmp_path / "collection" / "config" / "pipeline" / COLLECTION_NAME
    oe_dir.mkdir(parents=True)
    (oe_dir / "old-entity.csv").write_text(
        f"old-entity,entity,status\n"
        f"{entity_min},1,\n"          # in range — should be loaded
        f"{entity_min - 1},1,\n"      # out of range — should be filtered
    )

    return tmp_path / "collection"


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_command_exits_successfully(parquet_dir, collection_dir, specification_dir, tmp_path):
    """The CLI should complete with exit code 0 and create the output file."""
    output_path = tmp_path / "output" / f"{DATASET}.sqlite3"

    result = CliRunner().invoke(run_command, [
        "--dataset", DATASET,
        "--parquet-datasets-path", str(parquet_dir),
        "--collection-data-path", str(collection_dir),
        "--collection", COLLECTION_NAME,
        "--output-path", str(output_path),
        "--specification-dir", str(specification_dir),
    ])

    assert result.exit_code == 0, result.output
    assert output_path.exists()


def test_only_target_dataset_rows_are_loaded(parquet_dir, collection_dir, specification_dir, tmp_path):
    """Rows from other Hive partitions should not appear in the output."""
    output_path = tmp_path / "output" / f"{DATASET}.sqlite3"

    result = CliRunner().invoke(run_command, [
        "--dataset", DATASET,
        "--parquet-datasets-path", str(parquet_dir),
        "--collection-data-path", str(collection_dir),
        "--collection", COLLECTION_NAME,
        "--output-path", str(output_path),
        "--specification-dir", str(specification_dir),
    ])
    assert result.exit_code == 0, result.output

    conn = sqlite3.connect(str(output_path))
    count = conn.execute('SELECT COUNT(*) FROM "entity"').fetchone()[0]
    conn.close()

    # Only the 2 rows from DATASET, not the 1 from OTHER_DATASET
    assert count == 2


def test_correct_entity_data_is_loaded(parquet_dir, collection_dir, specification_dir, tmp_path):
    """The correct rows from the target dataset should be present."""
    output_path = tmp_path / "output" / f"{DATASET}.sqlite3"

    result = CliRunner().invoke(run_command, [
        "--dataset", DATASET,
        "--parquet-datasets-path", str(parquet_dir),
        "--collection-data-path", str(collection_dir),
        "--collection", COLLECTION_NAME,
        "--output-path", str(output_path),
        "--specification-dir", str(specification_dir),
    ])
    assert result.exit_code == 0, result.output

    conn = sqlite3.connect(str(output_path))
    rows = conn.execute(
        'SELECT entity, name, reference FROM "entity" ORDER BY entity'
    ).fetchall()
    conn.close()

    assert rows == [(1, "Entity One", "ref-1"), (2, "Entity Two", "ref-2")]


def test_fact_data_is_loaded(parquet_dir, collection_dir, specification_dir, tmp_path):
    """Facts for the target dataset should be loaded into the fact table."""
    output_path = tmp_path / "output" / f"{DATASET}.sqlite3"

    result = CliRunner().invoke(run_command, [
        "--dataset", DATASET,
        "--parquet-datasets-path", str(parquet_dir),
        "--collection-data-path", str(collection_dir),
        "--collection", COLLECTION_NAME,
        "--output-path", str(output_path),
        "--specification-dir", str(specification_dir),
    ])
    assert result.exit_code == 0, result.output

    conn = sqlite3.connect(str(output_path))
    rows = conn.execute(
        'SELECT fact, entity, field, value FROM "fact" ORDER BY fact'
    ).fetchall()
    conn.close()

    assert rows == [
        ("fact-1", 1, "name", "Entity One"),
        ("fact-2", 2, "reference", "ref-1"),
    ]


def test_tables_with_no_parquet_data_remain_empty(parquet_dir, collection_dir, specification_dir, tmp_path):
    """Tables with no parquet files should be created but left empty."""
    output_path = tmp_path / "output" / f"{DATASET}.sqlite3"

    result = CliRunner().invoke(run_command, [
        "--dataset", DATASET,
        "--parquet-datasets-path", str(parquet_dir),
        "--collection-data-path", str(collection_dir),
        "--collection", COLLECTION_NAME,
        "--output-path", str(output_path),
        "--specification-dir", str(specification_dir),
    ])
    assert result.exit_code == 0, result.output

    conn = sqlite3.connect(str(output_path))
    count = conn.execute('SELECT COUNT(*) FROM "issue"').fetchone()[0]
    conn.close()

    assert count == 0


def test_dataset_resource_csv_is_loaded(parquet_dir, collection_dir, specification_dir, tmp_path):
    """dataset-resource rows from the collection CSV should be loaded."""
    output_path = tmp_path / "output" / f"{DATASET}.sqlite3"

    result = CliRunner().invoke(run_command, [
        "--dataset", DATASET,
        "--parquet-datasets-path", str(parquet_dir),
        "--collection-data-path", str(collection_dir),
        "--collection", COLLECTION_NAME,
        "--output-path", str(output_path),
        "--specification-dir", str(specification_dir),
    ])
    assert result.exit_code == 0, result.output

    conn = sqlite3.connect(str(output_path))
    rows = conn.execute(
        'SELECT resource, entry_date, entry_count, line_count, mime_type FROM dataset_resource ORDER BY resource'
    ).fetchall()
    conn.close()

    assert rows == [
        ("abc123", None, 3, 4, "text/csv"),
        ("def456", None, 5, 6, "text/csv"),
        ("ghi789", "2023-06-19T00:29:55Z", 7, 8, "application/zip"),
    ]


def test_column_field_csv_is_loaded(parquet_dir, collection_dir, specification_dir, tmp_path):
    """column-field rows from the collection CSV should be loaded."""
    output_path = tmp_path / "output" / f"{DATASET}.sqlite3"

    result = CliRunner().invoke(run_command, [
        "--dataset", DATASET,
        "--parquet-datasets-path", str(parquet_dir),
        "--collection-data-path", str(collection_dir),
        "--collection", COLLECTION_NAME,
        "--output-path", str(output_path),
        "--specification-dir", str(specification_dir),
    ])
    assert result.exit_code == 0, result.output

    conn = sqlite3.connect(str(output_path))
    count = conn.execute('SELECT COUNT(*) FROM column_field').fetchone()[0]
    conn.close()

    assert count == 1


def test_old_entity_filters_by_entity_range(parquet_dir, collection_dir, specification_dir, tmp_path):
    """Only old-entity rows within the dataset entity range should be loaded."""
    output_path = tmp_path / "output" / f"{DATASET}.sqlite3"

    result = CliRunner().invoke(run_command, [
        "--dataset", DATASET,
        "--parquet-datasets-path", str(parquet_dir),
        "--collection-data-path", str(collection_dir),
        "--collection", COLLECTION_NAME,
        "--output-path", str(output_path),
        "--specification-dir", str(specification_dir),
    ])
    assert result.exit_code == 0, result.output

    conn = sqlite3.connect(str(output_path))
    old_entities = conn.execute('SELECT old_entity FROM old_entity').fetchall()
    conn.close()

    # Two rows written but only the in-range one should be loaded
    assert len(old_entities) == 1
    assert old_entities[0][0] is not None


def test_only_current_delta_version_is_loaded(parquet_dir, collection_dir, specification_dir, tmp_path):
    """Rewriting a dataset leaves the previous version's parquet files on disk until the
    table is vacuumed; only the files in the current version should be loaded."""
    _write_delta(
        parquet_dir / "entity",
        DATASET,
        _entity_table([1, 2], ["Entity One", "Entity Two"], ["ref-1", "ref-2"]),
    )
    partition_files = list((parquet_dir / "entity" / f"dataset={DATASET}").glob("*.parquet"))
    assert len(partition_files) == 2, "expected the replaced version's file to still be on disk"

    output_path = tmp_path / "output" / f"{DATASET}.sqlite3"

    result = CliRunner().invoke(run_command, [
        "--dataset", DATASET,
        "--parquet-datasets-path", str(parquet_dir),
        "--collection-data-path", str(collection_dir),
        "--collection", COLLECTION_NAME,
        "--output-path", str(output_path),
        "--specification-dir", str(specification_dir),
    ])
    assert result.exit_code == 0, result.output

    conn = sqlite3.connect(str(output_path))
    rows = conn.execute('SELECT entity FROM "entity" ORDER BY entity').fetchall()
    conn.close()

    assert rows == [(1,), (2,)]


def _write_fact_resource_with_counts(parquet_dir):
    """Facts for three entities, one of which is not in the entity table, plus a
    fact_resource row for an entity that is not in the entity table."""
    null_str3 = pa.array([None, None, None], type=pa.string())
    _write_delta(
        parquet_dir / "fact",
        DATASET,
        pa.table({
            "entity":           pa.array([1, 2, 3], type=pa.int64()),
            "entry_date":       null_str3,
            "fact":             pa.array(["fact-1", "fact-2", "fact-3"], type=pa.string()),
            "field":            pa.array(["name", "name", "name"], type=pa.string()),
            "priority":         pa.array([None, None, None], type=pa.int64()),
            "reference_entity": null_str3,
            "value":            pa.array(["Entity One", "Entity Two", "Entity Three"], type=pa.string()),
        }),
    )
    rows = [
        ("fact-1", "abc123", 1),
        ("fact-2", "abc123", 2),
        ("fact-missing", "abc123", 3),
        ("fact-1", "def456", 1),
        ("fact-1", "def456", 2),
        ("fact-3", "def456", 3),
    ]
    _write_delta(
        parquet_dir / "fact_resource",
        DATASET,
        pa.table({
            "fact":         pa.array([r[0] for r in rows], type=pa.string()),
            "entry_date":   pa.array([None] * len(rows), type=pa.string()),
            "entry_number": pa.array([r[2] for r in rows], type=pa.int64()),
            "priority":     pa.array([None] * len(rows), type=pa.int64()),
            "resource":     pa.array([r[1] for r in rows], type=pa.string()),
            "entity":       pa.array([1, 2, 9, 1, 1, 3], type=pa.int64()),
            "field":        pa.array(["name"] * len(rows), type=pa.string()),
        }),
    )


def test_entity_counts_are_the_distinct_entities_each_resource_supplies(
    parquet_dir, collection_dir, specification_dir, tmp_path
):
    """entity_count is replaced for resources that supply facts, counting only entities
    in the entity table; other resources keep their CSV value."""
    _write_fact_resource_with_counts(parquet_dir)
    output_path = tmp_path / "output" / f"{DATASET}.sqlite3"

    result = CliRunner().invoke(run_command, [
        "--dataset", DATASET,
        "--parquet-datasets-path", str(parquet_dir),
        "--collection-data-path", str(collection_dir),
        "--collection", COLLECTION_NAME,
        "--output-path", str(output_path),
        "--specification-dir", str(specification_dir),
    ])
    assert result.exit_code == 0, result.output

    conn = sqlite3.connect(str(output_path))
    rows = conn.execute("SELECT resource, entity_count FROM dataset_resource ORDER BY resource").fetchall()
    conn.close()

    assert rows == [("abc123", 2), ("def456", 1), ("ghi789", None)]


def test_entity_counts_match_dataset_package_add_counts(parquet_dir, collection_dir, specification_dir, tmp_path):
    """The counts must equal what DatasetPackage.add_counts computes from the loaded tables."""
    _write_fact_resource_with_counts(parquet_dir)
    output_path = tmp_path / "output" / f"{DATASET}.sqlite3"

    result = CliRunner().invoke(run_command, [
        "--dataset", DATASET,
        "--parquet-datasets-path", str(parquet_dir),
        "--collection-data-path", str(collection_dir),
        "--collection", COLLECTION_NAME,
        "--output-path", str(output_path),
        "--specification-dir", str(specification_dir),
    ])
    assert result.exit_code == 0, result.output

    query = "SELECT resource, entity_count FROM dataset_resource ORDER BY resource"
    conn = sqlite3.connect(str(output_path))
    built = conn.execute(query).fetchall()
    conn.close()

    DatasetPackage(
        DATASET,
        organisation=Organisation(organisation={}),
        path=str(output_path),
        specification_dir=str(specification_dir),
    ).add_counts()

    conn = sqlite3.connect(str(output_path))
    recounted = conn.execute(query).fetchall()
    conn.close()

    assert built == recounted
