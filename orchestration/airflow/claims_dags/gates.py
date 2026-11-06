"""Post-build quality gates on the gold Parquet export. ``python -m claims_dags.gates ...``

dbt tests already ran in the previous task; these are the cheap cross-checks the orchestrator
adds, keyed on the run's logical date (the batch):

* every star table exists and has rows;
* ``fact_claim.claim_id`` is unique and every ``fact_claim_line.claim_id`` exists in it;
* if the raw tree has a ``claims`` partition for the batch date, gold must hold rows with
  ``load_batch_id = bYYYYMMDD`` (a day with input must not silently load nothing).

Exit code 0 = pass, 1 = a gate failed (message on stderr), 2 = usage error.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from .spec import batch_key

TABLES = (
    "dim_date",
    "dim_member",
    "dim_provider",
    "dim_diagnosis",
    "fact_claim",
    "fact_claim_line",
)


def _read(export: Path, table: str, columns: list[str] | None = None):
    import pyarrow.dataset as ds

    d = export / table
    if not d.is_dir():
        raise FileNotFoundError(f"missing export table {d}")
    return ds.dataset(str(d), format="parquet").to_table(columns=columns)


def run_gates(gold: Path, raw: Path | None, batch_date: str) -> list[str]:
    """Return a list of failure messages (empty = all gates passed)."""
    export = gold / "export"
    bad: list[str] = []
    for t in TABLES:
        try:
            if _read(export, t).num_rows == 0:
                bad.append(f"{t}: export has zero rows")
        except FileNotFoundError as e:
            bad.append(str(e))
    if bad:
        return bad
    claims = _read(export, "fact_claim", ["claim_id", "load_batch_id"])
    ids = claims.column("claim_id").to_pylist()
    if len(set(ids)) != len(ids):
        bad.append(f"fact_claim: {len(ids) - len(set(ids))} duplicate claim_id values")
    lines = _read(export, "fact_claim_line", ["claim_id"]).column("claim_id").to_pylist()
    orphans = set(lines) - set(ids)
    if orphans:
        bad.append(f"fact_claim_line: {len(orphans)} claim_id values missing in fact_claim")
    if raw is not None and (raw / "claims" / f"ingest_date={batch_date}").is_dir():
        key = batch_key(batch_date)
        if key not in set(claims.column("load_batch_id").to_pylist()):
            bad.append(f"raw has claims for {batch_date} but gold has no rows for {key}")
    return bad


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="claims_dags.gates", description=__doc__.split("\n")[0])
    ap.add_argument("--gold", type=Path, required=True)
    ap.add_argument("--raw", type=Path, default=None)
    ap.add_argument("--batch-date", required=True, help="YYYY-MM-DD (the logical date)")
    a = ap.parse_args(argv)
    try:
        batch_key(a.batch_date)
    except ValueError:
        print(f"--batch-date must be YYYY-MM-DD, got {a.batch_date!r}", file=sys.stderr)
        return 2
    failures = run_gates(a.gold, a.raw, a.batch_date)
    for f in failures:
        print(f"GATE FAILED: {f}", file=sys.stderr)
    if not failures:
        print(f"quality gates passed for {a.batch_date} ({batch_key(a.batch_date)})")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
