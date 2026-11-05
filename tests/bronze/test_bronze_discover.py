"""Spark-free unit tests: path building, file discovery, CLI argument handling."""

from datetime import date

import pytest

from lakehouse.bronze.cli import main, parse_args
from lakehouse.bronze.datasets import DATASETS
from lakehouse.bronze.discover import available_dates, discover
from lakehouse.bronze.paths import table_uri


def test_table_uri_handles_trailing_slash_and_schemes():
    assert table_uri("/tmp/b", "claims") == "/tmp/b/claims"
    assert table_uri("s3a://bronze/", "_meta/schema_log") == "s3a://bronze/_meta/schema_log"


def _touch(root, rel):
    p = root / rel
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text("x")


def test_discover_prefers_parquet_over_csv_copies_and_picks_the_batch(tmp_path):
    _touch(tmp_path, "claims/ingest_date=2024-01-02/part-00000.parquet")
    _touch(tmp_path, "claims/ingest_date=2024-01-02/part-00000.csv")
    _touch(tmp_path, "claims/ingest_date=2024-01-03/part-00000.parquet")
    files = discover(tmp_path, DATASETS["claims"], date(2024, 1, 2))
    assert [f.rel for f in files] == ["claims/ingest_date=2024-01-02/part-00000.parquet"]


def test_discover_falls_back_to_csv_and_ignores_hidden_files(tmp_path):
    _touch(tmp_path, "claims/ingest_date=2024-01-02/part-00000.csv")
    _touch(tmp_path, "claims/ingest_date=2024-01-02/_SUCCESS")
    files = discover(tmp_path, DATASETS["claims"], date(2024, 1, 2))
    assert [f.fmt for f in files] == ["csv"]


def test_discover_snapshot_and_flat_datasets(tmp_path):
    _touch(tmp_path, "members/snapshot_date=2024-01-01/part-00000.parquet")
    _touch(tmp_path, "events/claim_events.jsonl")
    assert len(discover(tmp_path, DATASETS["members"], date(2024, 1, 1))) == 1
    assert discover(tmp_path, DATASETS["members"], date(2024, 1, 2)) == []
    assert len(discover(tmp_path, DATASETS["events"], date(2030, 1, 1))) == 1  # routed by row


def test_available_dates_sorted_unique(tmp_path):
    _touch(tmp_path, "claims/ingest_date=2024-01-03/a.parquet")
    _touch(tmp_path, "notes/ingest_date=2024-01-02/a.jsonl")
    _touch(tmp_path, "claims/ingest_date=2024-01-02/a.parquet")
    _touch(tmp_path, "claims/ingest_date=garbage/a.parquet")
    ds = [DATASETS["claims"], DATASETS["notes"]]
    assert available_dates(tmp_path, ds) == [date(2024, 1, 2), date(2024, 1, 3)]


def test_cli_args_and_validation(tmp_path, capsys):
    a = parse_args(["--bronze", "s3a://b", "--batch-date", "2024-01-02"])
    assert a.bronze == "s3a://b" and a.batch_date == "2024-01-02"
    with pytest.raises(SystemExit):
        parse_args(["--batch-date", "2024-01-02"])  # --bronze is required
    assert main(["--input", str(tmp_path), "--bronze", "x", "--batch-date", "nope"]) == 2
    assert (
        main(["--input", str(tmp_path), "--bronze", "x", "--batch-date", "all", "--datasets", "zz"])
        == 2
    )
