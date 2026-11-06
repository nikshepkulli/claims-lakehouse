"""Bronze ingestion tests. Need PySpark + Java: `make test-spark` (Docker) or CI `spark-tests`.

One shared SparkSession and one claimsgen fixture (1000 claims, seed 42) for the whole module.
"""

import json
import os
import shutil
import subprocess
import sys
from datetime import date, datetime
from pathlib import Path

import pytest

pytest.importorskip("pyspark")
pytest.importorskip("delta")

import pyarrow.parquet as pq  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from lakehouse.bronze.ingest import ingest_batch  # noqa: E402
from lakehouse.bronze.spark import build_spark  # noqa: E402

pytestmark = pytest.mark.spark

SRC = Path(__file__).resolve().parents[2] / "src"
META = ["_ingest_ts", "_source_file", "_batch_id", "_ingest_date"]


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    wh = tmp_path_factory.mktemp("warehouse")
    s = build_spark(
        "bronze-tests",
        master="local[2]",
        conf={
            "spark.sql.shuffle.partitions": "2",
            "spark.ui.enabled": "false",
            "spark.sql.warehouse.dir": str(wh),
            "spark.databricks.delta.snapshotPartitions": "2",
        },
    )
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


@pytest.fixture(scope="module")
def raw(tmp_path_factory):
    out = tmp_path_factory.mktemp("raw")
    subprocess.run(
        [sys.executable, "-m", "claimsgen", "--rows", "1000", "--seed", "42", "--out", str(out),
         "--overwrite", "--quiet"],
        check=True,
        env={**os.environ, "PYTHONPATH": os.pathsep.join([str(SRC), os.environ.get("PYTHONPATH", "")])},  # noqa: E501
    )  # fmt: skip
    return out


def _days(raw, name):
    return {date.fromisoformat(p.name.split("=")[1]) for p in (raw / name).iterdir()}


@pytest.fixture(scope="module")
def days(raw):
    """Two consecutive-ish batch days that both have claims and notes."""
    both = sorted(_days(raw, "claims") & _days(raw, "notes"))
    return both[0], both[1]


@pytest.fixture
def bronze(tmp_path):
    return str(tmp_path / "bronze")


def table(spark, bronze, name):
    return spark.read.format("delta").load(f"{bronze}/{name}")


def src_rows(raw, name, day):
    d = raw / name / f"ingest_date={day}"
    return sum(pq.read_metadata(p).num_rows for p in d.glob("*.parquet"))


def counts(spark, bronze):
    names = ["claims", "claim_lines", "diagnoses", "notes"]
    return {n: table(spark, bronze, n).count() for n in names}


def test_ingest_lands_all_datasets_with_metadata(days, spark, raw, bronze):
    FIRST_DAY, DAY2 = days
    ts = datetime(2025, 1, 1, 12, 0, 0)
    ingest_batch(spark, raw, bronze, FIRST_DAY, ingest_ts=ts)
    for name in ("claims", "claim_lines", "diagnoses"):
        df = table(spark, bronze, name)
        assert df.count() == src_rows(raw, name, FIRST_DAY) > 0
        assert df.columns[-4:] == META
        row = df.select(*META).first()
        assert row["_ingest_ts"] == ts
        assert row["_batch_id"] == f"b{FIRST_DAY:%Y%m%d}"
        assert row["_ingest_date"] == FIRST_DAY
        assert row["_source_file"].startswith(f"{name}/ingest_date={FIRST_DAY}/part-")
        assert df.where(" OR ".join(f"{c} IS NULL" for c in META)).count() == 0
    # unstructured: raw JSON line kept verbatim next to the parsed fields
    notes = table(spark, bronze, "notes")
    n_src = sum(
        1
        for p in (raw / "notes" / f"ingest_date={FIRST_DAY}").glob("*.jsonl")
        for ln in p.read_text().splitlines()
        if ln.strip()
    )
    assert notes.count() == n_src > 0
    r = notes.first()
    assert json.loads(r["_raw"])["text"] == r["text"] and "SYNTHETIC NOTE" in r["_raw"]
    # dims land on their snapshot date only
    assert not (Path(bronze) / "members").exists()


def test_snapshot_dims_and_events_routed_by_date(spark, raw, bronze):
    ingest_batch(spark, raw, bronze, date(2024, 1, 1))
    assert table(spark, bronze, "members").count() == 1000
    assert table(spark, bronze, "providers").count() == 200
    total = sum(1 for ln in (raw / "events/claim_events.jsonl").read_text().splitlines() if ln)
    ev_dates = {
        json.loads(ln)["event_ts"][:10]
        for ln in (raw / "events/claim_events.jsonl").read_text().splitlines()
        if ln
    }
    for d in sorted(ev_dates)[:3]:
        ingest_batch(spark, raw, bronze, date.fromisoformat(d), datasets=["events"])
    ev = table(spark, bronze, "events")
    assert 0 < ev.count() < total
    assert (
        ev.where(
            F.substring("event_ts", 1, 10) != F.date_format("_ingest_date", "yyyy-MM-dd")
        ).count()
        == 0
    )


def test_rerun_is_idempotent_and_isolated(days, spark, raw, bronze):
    FIRST_DAY, DAY2 = days
    ingest_batch(spark, raw, bronze, FIRST_DAY)
    ingest_batch(spark, raw, bronze, DAY2)
    before = counts(spark, bronze)
    # replay the first day with a later ingest timestamp
    ingest_batch(spark, raw, bronze, FIRST_DAY, ingest_ts=datetime(2030, 1, 1))
    assert counts(spark, bronze) == before
    c = table(spark, bronze, "claims")
    assert c.where(f"_batch_id = 'b{FIRST_DAY:%Y%m%d}'").select("_ingest_ts").distinct().first()[
        0
    ] == datetime(2030, 1, 1)
    # the other batch was untouched
    assert c.where(f"_batch_id = 'b{DAY2:%Y%m%d}'").select("_ingest_ts").distinct().first()[
        0
    ] != datetime(2030, 1, 1)
    # exact same rows after replay, including the line-level key
    assert c.where(f"_batch_id = 'b{FIRST_DAY:%Y%m%d}'").count() == src_rows(
        raw, "claims", FIRST_DAY
    )


def test_replay_replaces_changed_source_and_keeps_history(days, spark, raw, bronze, tmp_path):
    FIRST_DAY, DAY2 = days
    inp = tmp_path / "in"
    shutil.copytree(
        raw / "claims" / f"ingest_date={FIRST_DAY}", inp / "claims" / f"ingest_date={FIRST_DAY}"
    )
    ingest_batch(spark, inp, bronze, FIRST_DAY, datasets=["claims"])
    n = table(spark, bronze, "claims").count()
    f = next((inp / "claims" / f"ingest_date={FIRST_DAY}").glob("*.parquet"))
    t = pq.read_table(f)
    pq.write_table(t.slice(0, n - 1), f)  # corrected upstream file with one row fewer
    ingest_batch(spark, inp, bronze, FIRST_DAY, datasets=["claims"])
    assert table(spark, bronze, "claims").count() == n - 1
    old = spark.read.format("delta").option("versionAsOf", 0).load(f"{bronze}/claims")
    assert old.count() == n  # append-only history: the first load is still readable


def _drift_day(raw):
    return date.fromisoformat(
        json.loads((raw / "defects_manifest.json").read_text())["schema_drift"]["ingest_date"]
    )


def test_schema_drift_is_merged_and_logged_not_fatal(spark, raw, bronze):
    drift = _drift_day(raw)
    days = sorted(date.fromisoformat(p.name.split("=")[1]) for p in (raw / "claims").iterdir())
    prev = max(d for d in days if d < drift)
    ingest_batch(spark, raw, bronze, prev, datasets=["claims"])
    assert "prior_auth_flag" not in table(spark, bronze, "claims").columns
    res = ingest_batch(spark, raw, bronze, drift, datasets=["claims"])[0]
    assert res.columns_added == ["prior_auth_flag"] and res.files_quarantined == 0
    c = table(spark, bronze, "claims")
    assert "prior_auth_flag" in c.columns
    assert c.where(f"_ingest_date = DATE'{prev}' AND prior_auth_flag IS NOT NULL").count() == 0
    assert c.where(f"_ingest_date = DATE'{drift}' AND prior_auth_flag IS NOT NULL").count() > 0
    log = table(spark, bronze, "_meta/schema_log").where("dataset = 'claims'")
    by_batch = {r["batch_date"]: r for r in log.collect()}
    assert by_batch[drift]["drift_detected"] and by_batch[drift]["columns_added"] == [
        "prior_auth_flag"
    ]
    assert not by_batch[prev]["drift_detected"]
    observed = [f["name"] for f in json.loads(by_batch[drift]["observed_schema"])["fields"]]
    assert "prior_auth_flag" in observed
    # replaying the drift day neither duplicates nor loses the drift event
    n = c.count()
    ingest_batch(spark, raw, bronze, drift, datasets=["claims"])
    assert table(spark, bronze, "claims").count() == n
    log2 = table(spark, bronze, "_meta/schema_log").where(
        f"dataset = 'claims' AND batch_date = DATE'{drift}'"
    )
    assert log2.count() == 1 and log2.first()["drift_detected"]


def test_unreadable_files_and_bad_lines_are_quarantined(days, spark, raw, bronze, tmp_path):
    FIRST_DAY, DAY2 = days
    inp = tmp_path / "in"
    for name in ("claims", "notes"):
        shutil.copytree(
            raw / name / f"ingest_date={FIRST_DAY}", inp / name / f"ingest_date={FIRST_DAY}"
        )
    cdir = inp / "claims" / f"ingest_date={FIRST_DAY}"
    (cdir / "part-00001.parquet").write_bytes(b"this is not parquet")
    good_notes = sum(1 for p in (inp / "notes" / f"ingest_date={FIRST_DAY}").glob("*.jsonl")
                     for ln in p.read_text().splitlines() if ln.strip())  # fmt: skip
    npath = next((inp / "notes" / f"ingest_date={FIRST_DAY}").glob("*.jsonl"))
    with npath.open("a") as fh:
        fh.write('{"note_id": "broken\n')
    results = {
        r.dataset: r for r in ingest_batch(spark, inp, bronze, FIRST_DAY, ["claims", "notes"])
    }
    assert results["claims"].files_quarantined == 1 and results["notes"].rows_quarantined == 1
    assert table(spark, bronze, "claims").count() == src_rows(raw, "claims", FIRST_DAY)
    assert table(spark, bronze, "notes").count() == good_notes
    q = table(spark, bronze, "_meta/quarantine")
    rows = {r["dataset"]: r for r in q.collect()}
    assert rows["claims"]["source_file"].endswith("part-00001.parquet")
    assert rows["claims"]["reason"].startswith("unreadable file")
    assert rows["notes"]["payload"] == '{"note_id": "broken'
    # replay: quarantine rows are replaced, not duplicated
    ingest_batch(spark, inp, bronze, FIRST_DAY, ["claims", "notes"])
    assert q.count() == 2 and table(spark, bronze, "claims").count() == src_rows(
        raw, "claims", FIRST_DAY
    )


def _corrupt_first_data_page(path: Path) -> int:
    """Overwrite bytes inside the first column chunk; magic number and footer stay intact."""
    meta = pq.read_metadata(path)
    col = meta.row_group(0).column(0)
    start = col.data_page_offset
    size = col.total_compressed_size
    blob = bytearray(path.read_bytes())
    mid = start + size // 2
    blob[mid : mid + max(16, size // 4)] = b"\xff" * max(16, size // 4)
    path.write_bytes(bytes(blob))
    assert pq.read_metadata(path).num_rows == meta.num_rows  # footer still valid
    with pytest.raises(Exception):  # noqa: B017 - the page really is undecodable
        pq.read_table(path)
    return meta.num_rows


def test_page_corrupt_parquet_file_is_quarantined_and_batch_lands(
    days, spark, raw, bronze, tmp_path
):
    day = days[0]
    inp = tmp_path / "in"
    cdir = inp / "claims" / f"ingest_date={day}"
    shutil.copytree(raw / "claims" / f"ingest_date={day}", cdir)
    good_rows = src_rows(raw, "claims", day)
    bad = cdir / "part-00099.parquet"
    shutil.copy(next(cdir.glob("part-*.parquet")), bad)
    bad_rows = _corrupt_first_data_page(bad)

    r = ingest_batch(spark, inp, bronze, day, ["claims"])[0]
    assert r.files_quarantined == 1 and r.rows_written == good_rows
    assert r.quarantined_file_rows == bad_rows  # footer row count of the dropped file
    claims = table(spark, bronze, "claims")
    assert claims.count() == good_rows
    assert claims.where("_source_file LIKE '%part-00099%'").count() == 0
    q = table(spark, bronze, "_meta/quarantine").collect()
    assert len(q) == 1
    assert q[0]["source_file"].endswith("part-00099.parquet") and q[0]["payload"] is None
    assert q[0]["reason"].startswith("CORRUPT_DATA_PAGE:") and f"{bad_rows} rows" in q[0]["reason"]
    log = table(spark, bronze, "_meta/schema_log").first()
    assert log["files_quarantined"] == 1 and log["rows_written"] == good_rows

    # re-running the batch is idempotent
    r2 = ingest_batch(spark, inp, bronze, day, ["claims"])[0]
    assert r2.rows_written == good_rows and r2.files_quarantined == 1
    assert table(spark, bronze, "claims").count() == good_rows
    assert table(spark, bronze, "_meta/quarantine").count() == 1

    # once the file is removed (fixed upstream) the same batch lands clean and clears quarantine
    bad.unlink()
    r3 = ingest_batch(spark, inp, bronze, day, ["claims"])[0]
    assert r3.files_quarantined == 0 and table(spark, bronze, "_meta/quarantine").count() == 0


def test_csv_only_partition_lands_as_strings_and_type_conflicts_quarantine(
    days, spark, raw, bronze, tmp_path
):
    FIRST_DAY, DAY2 = days
    import pyarrow.csv as pacsv

    inp = tmp_path / "in"
    d1 = inp / "claims" / f"ingest_date={FIRST_DAY}"
    d1.mkdir(parents=True)
    pacsv.write_csv(
        pq.read_table(next((raw / "claims" / f"ingest_date={FIRST_DAY}").glob("*.parquet"))),
        d1 / "part-00000.csv",
    )
    ingest_batch(spark, inp, bronze, FIRST_DAY, ["claims"])
    assert dict(table(spark, bronze, "claims").dtypes)["total_paid_amount"] == "string"
    # a typed parquet file for another day conflicts with the string-typed table -> quarantined
    d2 = inp / "claims" / f"ingest_date={DAY2}"
    shutil.copytree(raw / "claims" / f"ingest_date={DAY2}", d2)
    r = ingest_batch(spark, inp, bronze, DAY2, ["claims"])[0]
    assert r.files_quarantined == 1 and r.rows_written == 0
    assert "incompatible column types" in table(spark, bronze, "_meta/quarantine").first()["reason"]


def test_missing_batch_is_a_noop(spark, raw, bronze):
    res = ingest_batch(spark, raw, bronze, date(1999, 1, 1), ["claims", "notes"])
    assert all(r.skipped for r in res) and not Path(bronze).exists()
