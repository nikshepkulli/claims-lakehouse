import os
import subprocess
import sys
from pathlib import Path

import pytest
from governance_helpers import SRC


@pytest.fixture(scope="session")
def raw_dir(tmp_path_factory) -> Path:
    """A small deterministic claimsgen run (synthetic). Pure Python + pyarrow, no Java."""
    out = tmp_path_factory.mktemp("gov_raw")
    env = {**os.environ, "PYTHONPATH": str(SRC)}
    subprocess.run(
        [sys.executable, "-m", "claimsgen", "--rows", "1000", "--seed", "7", "--out", str(out)],
        check=True,
        env=env,
        capture_output=True,
    )
    return out
