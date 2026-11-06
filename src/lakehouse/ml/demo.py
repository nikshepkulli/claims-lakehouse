"""``make ml-demo``: synthetic claims -> feature store -> train (twice) -> score (twice) -> report.

Fails (exit 1) if the two training runs disagree on a single metric byte, or if the second score
run rewrites anything. Everything is local; nothing talks to AWS.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

from .hyperparams import Hyperparameters
from .score import score
from .train import print_report, train


def run_demo(work: Path, rows: int = 20000, seed: int = 42) -> int:
    from lakehouse.features import store
    from lakehouse.features.fixture import from_claimsgen

    if work.exists():
        shutil.rmtree(work)
    src = Path(__file__).resolve().parents[2]
    env = {**os.environ, "PYTHONPATH": str(src)}
    subprocess.run(
        [sys.executable, "-m", "claimsgen", "--rows", str(rows), "--seed", str(seed),
         "--out", str(work / "raw"), "--quiet"],
        env=env, check=True,
    )  # fmt: skip
    from_claimsgen(work / "raw", work / "export")
    store.build(work / "export", work / "store", probes=2)

    hp = Hyperparameters(seed=seed)
    reports = []
    for i in (1, 2):
        reports.append(train(work / "store", work / f"model{i}", work / f"out{i}", hp))
    m1, m2 = ((work / f"out{i}" / "metrics.json").read_bytes() for i in (1, 2))
    print_report(reports[0])
    if m1 != m2:
        print("DETERMINISM FAILED: two training runs produced different metrics", file=sys.stderr)
        return 1
    print("determinism ok: two training runs produced byte-identical metrics.json")

    r1 = score(work / "store", work / "model1", work / "scores")
    h1 = (work / "scores" / "part-00000.parquet").read_bytes()
    r2 = score(work / "store", work / "model1", work / "scores")
    r3 = score(work / "store", work / "model1", work / "scores", force=True)
    ok = (
        r1["status"] == "written"
        and r2["status"] == "skipped"
        and r3["scores_sha256"] == r1["scores_sha256"]
        and (work / "scores" / "part-00000.parquet").read_bytes() == h1
    )
    print(f"scored {r1['rows']} claims -> {work / 'scores'} ({r1['model_version']})")
    if not ok:
        print("IDEMPOTENCY FAILED: re-scoring changed the output", file=sys.stderr)
        return 1
    print("idempotency ok: re-run skipped; forced re-run is byte-identical")
    (work / "demo_summary.json").write_text(
        json.dumps({"model_version": r1["model_version"], "rows_scored": r1["rows"]}, indent=2)
    )
    return 0
