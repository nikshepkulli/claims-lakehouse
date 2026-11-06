"""The hand-kept table lists must match the code they describe; schema facet sources."""

import re
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import yaml

from lakehouse.bronze.datasets import DATASETS
from lakehouse.gold import schema as G
from lakehouse.lineage import topology as T
from lakehouse.lineage.schemas import fields_for, parquet_fields

ROOT = Path(__file__).resolve().parents[2]


def _suite_tables(layer):
    return set(yaml.safe_load((ROOT / f"quality/expectations/{layer}.yaml").read_text())["tables"])


def test_raw_datasets_match_bronze_job():
    assert set(T.RAW_DATASETS) == set(DATASETS)


def test_silver_tables_match_the_silver_job_and_contract():
    src = (ROOT / "src/lakehouse/silver/datasets.py").read_text()
    assert set(re.findall(r"^        Spec\(\n\s+\"(\w+)\"", src, re.M)) == set(T.SILVER_TABLES)
    suite = _suite_tables("silver")
    assert set(T.SILVER_OPS) <= suite and suite <= set(T.SILVER_TABLES) | set(T.SILVER_OPS)


def test_gold_inputs_match_gold_job():
    src = (ROOT / "src/lakehouse/gold/job.py").read_text()
    m = re.search(r"^SILVER_INPUTS = \(([^)]*)\)", src, re.M)
    assert set(re.findall(r'"(\w+)"', m.group(1))) == set(T.GOLD_INPUTS)


def test_gold_ops_tables_match_gold_paths():
    from lakehouse.gold import paths

    assert set(T.GOLD_OPS) == {paths.RECONCILIATION, paths.EXCLUSIONS}
    assert set(T.GOLD_TABLES) == set(G.TABLES)


def test_layer_io_without_roots_is_empty_and_unknown_layer_too(roots):
    assert T.layer_io("gold", T.Roots()) == ([], [])
    assert T.layer_io("streaming", roots) == ([], [])


def test_schema_comes_from_contracts_per_layer():
    assert [n for n, _ in fields_for("claims", "silver")][:2] == ["claim_id", "member_id"]
    bronze = [n for n, _ in fields_for("claims", "bronze")]
    assert "_ingest_ts" in bronze  # bronze claims carries ingest metadata, silver does not
    assert "_ingest_ts" not in [n for n, _ in fields_for("claims", "silver")]
    assert fields_for("no_such_table", "silver") == []


def test_parquet_schema_inference_adds_hive_partition_columns(tmp_path):
    part = tmp_path / "fact" / "load_batch_id=b20240105"
    part.mkdir(parents=True)
    pq.write_table(pa.table({"a": [1], "b": ["x"]}), part / "part-0.parquet")
    (tmp_path / "fact" / "_delta_log").mkdir()
    assert parquet_fields(tmp_path / "fact") == [
        ("a", "int64"),
        ("b", "string"),
        ("load_batch_id", "string"),
    ]
    assert fields_for("fact", None, tmp_path / "fact")[0] == ("a", "int64")
    assert parquet_fields(tmp_path / "missing") == []


def test_corrupt_parquet_gives_no_schema_instead_of_failing(tmp_path):
    (tmp_path / "t").mkdir()
    (tmp_path / "t" / "part.parquet").write_bytes(b"not parquet")
    assert parquet_fields(tmp_path / "t") == []
