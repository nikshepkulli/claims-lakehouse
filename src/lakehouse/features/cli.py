"""``python -m lakehouse.features`` : build, verify or inspect the feature store."""

from __future__ import annotations

import argparse
import sys

from . import store
from .config import FeatureConfig
from .guards import LeakageError


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="lakehouse.features", description=__doc__)
    ap.add_argument("--gold-export", default="data/gold-delta/export", help="gold Parquet export")
    ap.add_argument("--out", default="data/gold-delta/features", help="feature store root")
    ap.add_argument("--outcome-lag-days", type=int, default=FeatureConfig.outcome_lag_days)
    ap.add_argument("--probes", type=int, default=4, help="leakage-guard probe moments")
    ap.add_argument("--no-guard", action="store_true", help="skip the leakage guard (not for CI)")
    ap.add_argument("--verify", action="store_true", help="only verify stored files vs manifest")
    a = ap.parse_args(argv)
    if a.verify:
        m = store.verify(a.out)
        print(f"verified {len(m['tables'])} tables in {a.out}")
        return 0
    try:
        m = store.build(
            a.gold_export,
            a.out,
            FeatureConfig(outcome_lag_days=a.outcome_lag_days),
            guard=not a.no_guard,
            probes=a.probes,
        )
    except LeakageError as e:
        print(f"LEAKAGE GUARD FAILED: {e}", file=sys.stderr)
        return 1
    for name, t in m["tables"].items():
        print(f"{name}: {t['rows']} rows  sha256={t['sha256'][:12]}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
