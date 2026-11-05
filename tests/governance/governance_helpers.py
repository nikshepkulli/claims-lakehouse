"""Shared constants for tests/governance (kept out of conftest.py, whose module name collides
across test directories)."""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

TEST_KEY = "unit-test-key-0123456789-abcdefghij"  # synthetic, test-only, not a secret
OTHER_KEY = "rotated-test-key-9876543210-zyxwvuts"
