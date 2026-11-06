"""Shared fixtures: generate claimsgen data, the gold test fixture, then build the site once."""

from __future__ import annotations

import json
import re
import subprocess
import sys
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))  # import the top-level `dashboard` package


@pytest.fixture(scope="session")
def built(tmp_path_factory):
    """Build the dashboard against the 5,000-claim gold test fixture. Skips without the deps."""
    pytest.importorskip("duckdb")
    pytest.importorskip("pyarrow")
    pytest.importorskip("numpy")
    pytest.importorskip("dbt.cli.main")
    work = tmp_path_factory.mktemp("dash")
    env = {"PYTHONPATH": str(ROOT / "src"), "PATH": "/usr/bin:/bin"}
    subprocess.run(
        [sys.executable, "-m", "claimsgen", "--rows", "5000", "--seed", "42", "--quiet",
         "--out", str(work / "raw")],
        check=True, env=env, cwd=ROOT, capture_output=True,
    )  # fmt: skip
    subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "make_gold_fixture.py"),
         "--raw", str(work / "raw"), "--out", str(work / "gold")],
        check=True, cwd=ROOT, capture_output=True,
    )  # fmt: skip
    from dashboard.build import build

    t0 = time.monotonic()
    index = build(work / "gold", work / "site", raw=work / "raw")
    elapsed = time.monotonic() - t0
    html = index.read_text(encoding="utf-8")
    m = re.search(r'<script type="application/json" id="dashboard-data">(.*?)</script>', html, re.S)
    return {
        "work": work,
        "gold": work / "gold",
        "raw": work / "raw",
        "index": index,
        "html": html,
        "payload": json.loads(m.group(1)),
        "seconds": elapsed,
    }


@pytest.fixture(scope="session")
def sample(tmp_path_factory):
    """Run dashboard.sample (real observability / lineage / quality CLIs) once. Skips w/o deps."""
    pytest.importorskip("great_expectations")
    pytest.importorskip("deltalake")
    pytest.importorskip("pyarrow")
    pytest.importorskip("yaml")
    out = tmp_path_factory.mktemp("sample")
    r = subprocess.run(
        [sys.executable, "-m", "dashboard.sample", "--out", str(out)],
        cwd=ROOT, capture_output=True, text=True, check=False,
    )  # fmt: skip
    assert r.returncode == 0, r.stdout + r.stderr
    return out


@pytest.fixture(scope="session")
def pipeline_built(built, sample):
    """The page rebuilt with every pipeline input from the sample run."""
    from dashboard import pipeline
    from dashboard.build import build

    pipe = pipeline.pipeline_section(
        events=sample / "lineage" / "events.jsonl",
        column_lineage=ROOT / "lineage" / "column_lineage.yaml",
        runs=sample / "ops" / "pipeline_runs",
        alerts=sample / "ops" / "alerts.jsonl",
        quality_history=[sample / "dq"],
        label="sample run from the synthetic fixture",
    )
    index = build(built["gold"], built["work"] / "site-pipeline", raw=built["raw"], pipe=pipe)
    html = index.read_text(encoding="utf-8")
    m = re.search(r'<script type="application/json" id="dashboard-data">(.*?)</script>', html, re.S)
    return {
        "html": html,
        "payload": json.loads(m.group(1)),
        "pipe": json.loads(m.group(1))["pipeline"],
    }
