import sys
from pathlib import Path

SRC = Path(__file__).resolve().parents[2] / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import pytest  # noqa: E402

# GE, pandas and delta-rs come from requirements-quality.txt. Skip (not fail) where they are not
# installed, for example on a Python without wheels; CI installs them, so the suite runs there.
pytest.importorskip("great_expectations")
pytest.importorskip("deltalake")
pytest.importorskip("pandas")
