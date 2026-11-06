"""Generate the SAMPLE inputs for the pipeline-runs view (ticket #30) from real local CLI runs.

    python -m dashboard.sample --out data/dashboard-sample

Nothing here is typed in or simulated: every file is written by the repository's own CLIs, run
on the synthetic fixture (no Spark, no Docker, no network):

1. ``lakehouse.observability wrap`` around each step writes the run ledger (Parquet); the step
   reports its row counts through ``LAKEHOUSE_RUN_METRICS_FILE``;
2. ``lakehouse.lineage wrap`` (inside the observability wrap, the documented nesting) writes the
   OpenLineage events (JSON lines);
3. ``lakehouse.quality run`` is the Great Expectations gold runner; ``lakehouse.lineage quality``
   turns each of its results into a ``quality.gold`` run;
4. ``lakehouse.observability check`` evaluates freshness, volume and failure rules over the ledger.

Steps (batch ``sample-1``): ``claimsgen`` (landing files), ``gold_fixture`` (landing -> gold star
tables via ``scripts/make_gold_fixture.py``; this is the fixture, not the Spark bronze/silver/gold
jobs), ``quality_gold``. Batch ``sample-2`` runs the same quality check on a copy of gold with a
planted duplicate claim and negated line amounts, so the trend shows a real change. Row counts,
statuses and expectation counts are deterministic for the seed; timestamps and durations are
measured, so they differ between runs.

Layout of ``--out``: ``ops/pipeline_runs/*.parquet`` (ledger), ``ops/alerts.jsonl``,
``lineage/events.jsonl``, ``dq/<batch>/gold-result.json``, ``observability.yaml`` (config used:
``expected_tables`` narrowed to the tables this sample runs), plus ``raw/`` and ``gold/``.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
SEED, ROWS = 42, 5000
RAW_TABLES = ("claims", "claim_lines", "members", "providers", "diagnoses")
GOLD_TABLES = (
    "dim_date",
    "dim_member",
    "dim_provider",
    "dim_diagnosis",
    "fact_claim",
    "fact_claim_line",
)
SAMPLE_LABEL = "sample run from the synthetic fixture"


def _env() -> dict[str, str]:
    env = dict(os.environ)
    env["PYTHONPATH"] = str(ROOT / "src") + os.pathsep + env.get("PYTHONPATH", "")
    env.pop("LAKEHOUSE_RUN_ID", None)
    return env


def _run(cmd: list[str], *, ok: tuple[int, ...] = (0,)) -> int:
    p = subprocess.run(cmd, cwd=ROOT, env=_env(), check=False, capture_output=True, text=True)
    if p.returncode not in ok:
        sys.stderr.write(p.stdout + p.stderr)
        raise SystemExit(f"sample: {' '.join(cmd[:6])} exited {p.returncode}")
    return p.returncode


def _count_rows(path: Path) -> int:
    import pyarrow.parquet as pq

    return sum(pq.ParquetFile(f).metadata.num_rows for f in sorted(path.rglob("*.parquet")))


def step(kind: str, out: Path) -> None:
    """Metrics-reporting child that runs under ``wrap``."""
    raw, gold = out / "raw", out / "gold"
    if kind == "claimsgen":
        shutil.rmtree(raw, ignore_errors=True)
        _run(
            [sys.executable, "-m", "claimsgen", "--rows", str(ROWS), "--seed", str(SEED)]
            + ["--out", str(raw), "--quiet"]
        )
        metrics = {"rows_out": sum(_count_rows(raw / t) for t in RAW_TABLES)}
    elif kind == "gold_fixture":
        shutil.rmtree(gold, ignore_errors=True)
        _run(
            [sys.executable, str(ROOT / "scripts" / "make_gold_fixture.py")]
            + ["--raw", str(raw), "--out", str(gold)]
        )
        metrics = {
            "rows_in": sum(_count_rows(raw / t) for t in RAW_TABLES),
            "rows_out": sum(_count_rows(gold / t) for t in GOLD_TABLES),
        }
    else:
        raise SystemExit(f"unknown step {kind}")
    target = os.environ.get("LAKEHOUSE_RUN_METRICS_FILE")
    if target:
        Path(target).write_text(json.dumps(metrics))


def _plant_defects(src: Path, dst: Path) -> None:
    """Copy gold; duplicate one claim and negate line amounts (as tests/dashboard plant)."""
    import duckdb

    shutil.rmtree(dst, ignore_errors=True)
    shutil.copytree(src, dst)
    con = duckdb.connect()
    claims = (dst / "fact_claim" / "part-00000.parquet").as_posix()
    lines = (dst / "fact_claim_line" / "part-00000.parquet").as_posix()
    con.execute(
        f"COPY (SELECT * FROM read_parquet('{claims}') UNION ALL "
        f"(SELECT * FROM read_parquet('{claims}') LIMIT 1)) TO '{claims}.new' (FORMAT parquet)"
    )
    con.execute(
        f"COPY (SELECT * REPLACE (-billed_amount AS billed_amount) FROM read_parquet('{lines}')) "
        f"TO '{lines}.new' (FORMAT parquet)"
    )
    Path(claims + ".new").replace(claims)
    Path(lines + ".new").replace(lines)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--out", type=Path, default=Path("data/dashboard-sample"))
    ap.add_argument("--step", help=argparse.SUPPRESS)
    a = ap.parse_args(argv)
    out = a.out.resolve()
    if a.step:
        step(a.step, out)
        return 0
    shutil.rmtree(out, ignore_errors=True)
    out.mkdir(parents=True)
    ops, events, py = out / "ops", out / "lineage" / "events.jsonl", sys.executable
    cfg = yaml.safe_load((ROOT / "config" / "observability.yaml").read_text())
    cfg["expected_tables"] = ["raw", "gold", "quality_gold"]  # only what this sample runs
    cfg["ledger_root"] = str(ops)
    cfg["sinks"]["file"]["path"] = str(ops / "alerts.jsonl")
    (out / "observability.yaml").write_text(yaml.safe_dump(cfg, sort_keys=False))
    obs = [py, "-m", "lakehouse.observability", "--config", str(out / "observability.yaml")]
    lin = [py, "-m", "lakehouse.lineage"]
    me = [py, "-m", "dashboard.sample", "--out", str(out), "--step"]
    raw, gold = out / "raw", out / "gold"

    def wrapped(layer: str, job: str, table: str, lineage: list[str], child: list[str]) -> None:
        _run(
            [*obs, "wrap", "--layer", layer, "--job", job, "--table", table]
            + ["--batch-id", "sample-1", "--"]
            + [*lin, "wrap", "--job", job, "--file", str(events), "--silver", str(out / "silver")]
            + [*lineage, "--", *child]
        )

    wrapped(
        "other", "claimsgen", "raw",
        ["--outputs", *[f"landing={raw / t}" for t in RAW_TABLES]],
        [*me, "claimsgen"],
    )  # fmt: skip
    wrapped(
        "gold", "gold_fixture", "gold",
        ["--inputs", *[f"landing={raw / t}" for t in RAW_TABLES],
         "--outputs", *[f"gold={gold / t}" for t in GOLD_TABLES]],
        [*me, "gold_fixture"],
    )  # fmt: skip
    planted = out / "gold-planted"
    _plant_defects(gold, planted)
    for batch, root in (("sample-1", gold), ("sample-2", planted)):
        dq = out / "dq" / batch
        quality = [py, "-m", "lakehouse.quality", "run", "--layer", "gold", "--root", str(root)]
        quality += ["--out", str(dq), "--no-docs"]
        # exit 1 is a real result (blocking failures), 2 is a usage error
        rc = _run(
            [*obs, "wrap", "--layer", "quality", "--job", "quality_gold"]
            + ["--table", "quality_gold", "--batch-id", batch, "--", *quality],
            ok=(0, 1),
        )
        res = dq / "gold-result.json"
        if not res.exists():
            raise SystemExit(f"sample: no result at {res} (quality exit {rc})")
        # sample-2 checked a copy of gold; its dataset identity is still the gold tables
        _run([*lin, "quality", "--result", str(res), "--gold", str(gold), "--file", str(events)])
    p = subprocess.run(
        [*obs, "--ops-root", str(ops), "check", "--no-dispatch", "--fail-on", "never"],
        cwd=ROOT, env=_env(), check=True, capture_output=True, text=True,
    )  # fmt: skip
    lines = [ln for ln in p.stdout.splitlines() if ln.strip().startswith("{")]
    (ops / "alerts.jsonl").write_text("".join(ln + "\n" for ln in lines))
    shutil.rmtree(planted, ignore_errors=True)
    print(f"wrote sample inputs to {out} ({SAMPLE_LABEL}); {len(lines)} alert(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
