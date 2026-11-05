import importlib.util
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))


def load_fixture_module():
    spec = importlib.util.spec_from_file_location(
        "make_gold_fixture", ROOT / "scripts" / "make_gold_fixture.py"
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod
