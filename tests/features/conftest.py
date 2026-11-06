import os
import subprocess
import sys
from pathlib import Path

import pytest

SRC = Path(__file__).resolve().parents[2] / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
sys.path.insert(0, str(Path(__file__).parent))

pytest.importorskip("duckdb")
pytest.importorskip("pyarrow")


@pytest.fixture(scope="session")
def gold_export(tmp_path_factory) -> Path:
    """A gold-shaped export built (without Spark) from a small deterministic claimsgen run.
    ~10% of members/providers have a second SCD2 version on 2024-07-01. All synthetic."""
    raw = tmp_path_factory.mktemp("feat_raw")
    env = {**os.environ, "PYTHONPATH": str(SRC)}
    r = subprocess.run(
        [sys.executable, "-m", "claimsgen", "--rows", "4000", "--seed", "11", "--out", str(raw)],
        env=env,
        capture_output=True,
        text=True,
    )
    assert r.returncode == 0, r.stderr[-2000:]
    from lakehouse.features.fixture import from_claimsgen

    return from_claimsgen(raw, tmp_path_factory.mktemp("feat_gold") / "export", seed=11)
