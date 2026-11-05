"""Tests for the synthetic claims generator: determinism, manifest accuracy,
referential integrity of non-injected rows, and the CLI."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest

from claimsgen import GenConfig, generate
from claimsgen.reference import ICD_RE, PROC_RE

ROWS, CHUNK = 20_000, 7_000  # 3 chunks: exercises chunk boundaries


def read(out: Path, name: str) -> pa.Table:
    files = sorted((out / name).rglob("*.parquet"))
    return pa.concat_tables([pq.read_table(f) for f in files], promote_options="default")


def tree_hash(out: Path) -> str:
    h = hashlib.sha256()
    for f in sorted(p for p in out.rglob("*") if p.is_file()):
        h.update(str(f.relative_to(out)).encode())
        h.update(f.read_bytes())
    return h.hexdigest()


def distinct(t: pa.Table, mask: pa.Array, col: str) -> int:
    return len(pc.unique(t.filter(mask)[col]))


def invalid(arr: pa.ChunkedArray, regex) -> pa.Array:
    ok = np.array([bool(regex.match(v)) if v is not None else True for v in arr.to_pylist()])
    return pa.array(~ok)


@pytest.fixture(scope="module")
def run(tmp_path_factory):
    out = tmp_path_factory.mktemp("raw")
    manifest = generate(GenConfig(rows=ROWS, out=out, seed=7, chunk_size=CHUNK))
    return out, manifest


def test_determinism_same_seed_identical_bytes(run, tmp_path):
    out, _ = run
    generate(GenConfig(rows=ROWS, out=tmp_path / "again", seed=7, chunk_size=CHUNK))
    assert tree_hash(out) == tree_hash(tmp_path / "again")


def test_different_seed_differs(run, tmp_path):
    out, _ = run
    generate(GenConfig(rows=ROWS, out=tmp_path / "o", seed=8, chunk_size=CHUNK))
    assert tree_hash(out) != tree_hash(tmp_path / "o")


def test_manifest_counts_match_data(run):
    out, m = run
    d = m["defects"]
    claims, lines, dx = read(out, "claims"), read(out, "claim_lines"), read(out, "diagnoses")
    members = set(read(out, "members")["member_id"].to_pylist())

    assert claims.num_rows - len(pc.unique(claims["claim_id"])) == d["duplicate_claim"]["count"]
    lag = pc.days_between(claims["service_date"], claims["received_date"])
    assert distinct(claims, pc.greater_equal(lag, 30), "claim_id") == d["late_arrival"]["count"]
    assert distinct(claims, pa.array([v not in members for v in claims["member_id"].to_pylist()]),
                    "claim_id") == d["orphan_member_id"]["count"]
    assert distinct(lines, pc.is_null(lines["procedure_code"]), "line_id") == d["null_procedure_code"]["count"]
    assert distinct(lines, invalid(lines["procedure_code"], PROC_RE), "line_id") == d["invalid_procedure_code"]["count"]
    assert distinct(lines, pc.less(lines["billed_amount"], 0), "line_id") == d["negative_amount"]["count"]
    dx = dx.append_column("k", pc.binary_join_element_wise(dx["claim_id"], pc.cast(dx["dx_seq"], pa.string()), "-"))
    assert distinct(dx, pc.is_null(dx["icd10_code"]), "k") == d["null_icd10"]["count"]
    assert distinct(dx, invalid(dx["icd10_code"], ICD_RE), "k") == d["invalid_icd10"]["count"]
    assert all(v["count"] > 0 for v in d.values()), "every defect type should fire at 20k rows"


def test_manifest_written_and_row_counts(run):
    out, m = run
    assert json.loads((out / "defects_manifest.json").read_text()) == m
    assert "100% synthetic" in m["synthetic_notice"]
    assert m["rows_written"]["claims"] == ROWS + m["defects"]["duplicate_claim"]["count"]
    assert m["rows_written"]["events"] == 2 * ROWS


def test_referential_integrity_of_non_injected_rows(run):
    out, m = run
    claims, lines, dx = read(out, "claims"), read(out, "claim_lines"), read(out, "diagnoses")
    ids = set(claims["claim_id"].to_pylist())
    assert set(lines["claim_id"].to_pylist()) <= ids
    assert set(dx["claim_id"].to_pylist()) <= ids
    assert set(claims["provider_id"].to_pylist()) <= set(read(out, "providers")["provider_id"].to_pylist())
    members = set(read(out, "members")["member_id"].to_pylist())
    orphans = {c for c, mid in zip(claims["claim_id"].to_pylist(), claims["member_id"].to_pylist())
               if mid not in members}
    assert len(orphans) == m["defects"]["orphan_member_id"]["count"]  # nothing else is orphaned
    # every claim has >=1 line and >=1 diagnosis; header totals reconcile to (deduped) lines
    first = np.unique(np.array(lines["line_id"].to_pylist()), return_index=True)[1]
    uniq = lines.take(pa.array(first))
    agg = uniq.group_by("claim_id").aggregate([("billed_amount", "sum")]).to_pylist()
    assert len(agg) == ROWS
    got = {r["claim_id"]: r["billed_amount_sum"] for r in agg}
    for cid, tot in zip(claims["claim_id"].to_pylist()[:2000], claims["total_billed_amount"].to_pylist()[:2000]):
        assert abs(got[cid] - tot) < 0.02


def test_duplicates_are_exact_resends(run):
    out, _ = run
    claims = read(out, "claims")
    dup_ids = [x["values"] for x in claims["claim_id"].value_counts().to_pylist() if x["counts"] > 1]
    assert dup_ids
    rows = claims.filter(pc.is_in(claims["claim_id"], pa.array(dup_ids[:50]))).to_pylist()
    by = {}
    for r in rows:
        by.setdefault(r["claim_id"], []).append(r)
    assert all(len(v) == 2 and v[0] == v[1] for v in by.values())


def test_schema_drift_in_exactly_one_day(run):
    out, m = run
    drift = m["schema_drift"]
    with_col = [f.parent.name for f in (out / "claims").rglob("*.parquet")
                if drift["added_column"] in pq.read_schema(f).names]
    assert set(with_col) == {f"ingest_date={drift['ingest_date']}"}
    assert any(drift["added_column"] in (f.read_text().splitlines()[0])
               for f in (out / "claims" / f"ingest_date={drift['ingest_date']}").glob("*.csv"))


def test_partition_matches_received_date_and_late_rows_land_late(run):
    out, _ = run
    for f in sorted((out / "claims").rglob("*.parquet"))[:60]:
        t = pq.read_table(f)
        assert {d.isoformat() for d in pc.unique(t["received_date"]).to_pylist()} == \
               {f.parent.name.split("=")[1]}


def test_realistic_skew(run):
    out, _ = run
    claims, prov = read(out, "claims"), read(out, "providers")
    tier = dict(zip(prov["provider_id"].to_pylist(), prov["cost_tier"].to_pylist()))
    billed = np.array(claims["total_billed_amount"].to_numpy())
    hi = np.array([tier[p] == "HIGH_COST" for p in claims["provider_id"].to_pylist()])
    assert 0 < hi.mean() < 0.15  # a few providers...
    assert billed[hi].mean() > 3 * billed[~hi].mean()  # ...with far higher cost
    months = np.array([d.month for d in claims["service_date"].to_pylist()])
    assert (months <= 2).sum() / 2 > 1.15 * (months == 7).sum()  # winter peak vs summer


def test_events_and_notes_are_synthetic_and_parseable(run):
    out, m = run
    evs = [json.loads(line) for line in (out / "events" / "claim_events.jsonl").read_text().splitlines()]
    assert len(evs) == 2 * ROWS
    assert {e["event_type"] for e in evs} == {"claim_submitted", "claim_paid", "claim_denied"}
    notes = [json.loads(line) for f in (out / "notes").rglob("*.jsonl") for line in f.read_text().splitlines()]
    assert len(notes) == m["rows_written"]["notes"] > 0
    assert all("SYNTHETIC" in n["text"] for n in notes)


def test_csv_parquet_row_parity(run):
    out, _ = run
    csv_rows = sum(len(f.read_text().splitlines()) - 1 for f in (out / "claim_lines").rglob("*.csv"))
    assert csv_rows == read(out, "claim_lines").num_rows


def _cli(*args, cwd=None):
    env = {**os.environ, "PYTHONPATH": str(Path(__file__).resolve().parents[2] / "src")}
    return subprocess.run([sys.executable, "-m", "claimsgen", *args], capture_output=True, text=True,
                          env=env, cwd=cwd)


def test_cli_smoke(tmp_path):
    out = tmp_path / "raw"
    r = _cli("--rows", "1000", "--seed", "1", "--out", str(out), "--quiet")
    assert r.returncode == 0, r.stderr
    assert (out / "defects_manifest.json").exists() and (out / "claims").is_dir()
    again = _cli("--rows", "1000", "--out", str(out), "--quiet")
    assert again.returncode == 2 and "--overwrite" in again.stderr
    assert _cli("--rows", "1000", "--out", str(out), "--quiet", "--overwrite").returncode == 0
    assert _cli("--rows", "10", "--out", str(tmp_path / "x")).returncode == 2
