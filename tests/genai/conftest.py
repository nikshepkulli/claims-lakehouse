"""Shared fixtures: a small synthetic gold fixture (claimsgen -> make_gold_fixture, no Spark)."""

import os
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tests" / "dbt"))

pytest.importorskip("duckdb")
pytest.importorskip("sqlglot")

from dbt_fixture_loader import load_fixture_module  # noqa: E402

from lakehouse.genai.catalog import build_catalog  # noqa: E402
from lakehouse.genai.config import GenAIConfig  # noqa: E402
from lakehouse.genai.executor import Executor  # noqa: E402


@pytest.fixture(scope="session")
def gold_dir(tmp_path_factory) -> Path:
    base = tmp_path_factory.mktemp("genai_gold")
    raw, out = base / "raw", base / "gold"
    env = {**os.environ, "PYTHONPATH": str(ROOT / "src")}
    subprocess.run(
        [sys.executable, "-m", "claimsgen", "--rows", "1000", "--seed", "7", "--out", str(raw)],
        check=True,
        env=env,
        capture_output=True,
    )
    load_fixture_module().build(raw, out)
    return out


@pytest.fixture(scope="session")
def analyst_catalog():
    return build_catalog("analyst")


@pytest.fixture(scope="session")
def analyst_executor(gold_dir, analyst_catalog):
    ex = Executor(analyst_catalog, gold_dir)
    yield ex
    ex.close()


@pytest.fixture()
def cfg() -> GenAIConfig:
    return GenAIConfig(role="analyst", session_max_queries=10_000)
