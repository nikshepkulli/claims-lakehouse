"""``python -m lakehouse.quality run --layer silver --root data/e2e/silver``.

Exit codes: 0 = no blocking failure (warnings are printed), 1 = a blocking expectation failed or a
required table is missing, 2 = usage error or the suite file could not be loaded.
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

from .suites import LAYERS, SuiteError


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="python -m lakehouse.quality", description=__doc__)
    sub = p.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run", help="validate one layer against its Great Expectations suites")
    r.add_argument("--layer", required=True, choices=LAYERS)
    r.add_argument("--root", required=True, help="layer root, e.g. data/e2e/silver (Delta/Parquet)")
    r.add_argument("--out", default="data/dq", help="result JSON + data docs directory")
    r.add_argument(
        "--expectations", default=None, help="suite directory (default quality/expectations)"
    )
    r.add_argument("--now", default=None, help="ISO time for freshness checks (default: UTC now)")
    r.add_argument("--no-docs", action="store_true", help="skip building the HTML data docs")
    r.add_argument(
        "--fail-on-warn", action="store_true", help="treat warn-severity failures as blocking"
    )
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        from .runner import report, run_layer
        from .suites import load_layer

        spec = load_layer(args.layer, args.expectations)
        now = datetime.fromisoformat(args.now) if args.now else None
        result = run_layer(spec, args.root, Path(args.out), now=now, docs=not args.no_docs)
    except (SuiteError, ValueError) as e:
        print(f"[dq] error: {e}", file=sys.stderr)
        return 2
    print(report(result))
    if args.fail_on_warn and result.failures("warn"):
        return 1
    return result.exit_code


if __name__ == "__main__":
    sys.exit(main())
