import sys
from pathlib import Path

SRC = Path(__file__).resolve().parents[2] / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


import pytest  # noqa: E402


@pytest.fixture(scope="session")
def generated_events(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Real generator output (1,000 claims, defects on): the contract must accept all of it."""
    from claimsgen.cli import main

    out = tmp_path_factory.mktemp("gen")
    assert main(["--rows", "1000", "--out", str(out), "--quiet"]) == 0
    return out / "events" / "claim_events.jsonl"
