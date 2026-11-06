"""``python -m lakehouse.ml {train,score,demo}``.

With ``--store`` given the commands use plain paths (local runs). Without it they follow the
SageMaker container contract: ``train`` reads ``<sm-root>/input/data/training`` and
``hyperparameters.json`` and writes ``<sm-root>/model`` and ``<sm-root>/output/data``; ``score``
follows the Processing-job layout (``/opt/ml/processing/{input/features,input/model,output}``).
A training failure is written to ``<sm-root>/output/failure`` and exits non-zero.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from . import sagemaker_env
from .hyperparams import Hyperparameters

PROCESSING = Path("/opt/ml/processing")


def _hp_args(pairs: list[str]) -> dict:
    out = {}
    for p in pairs or []:
        k, sep, v = p.partition("=")
        if not sep:
            raise SystemExit(f"--hp expects key=value, got {p!r}")
        out[k] = v
    return out


def cmd_train(a) -> int:
    from .train import print_report, train

    if a.store:
        hp = Hyperparameters.from_mapping(_hp_args(a.hp))
        report = train(a.store, a.model_dir, a.output_dir, hp)
        print_report(report)
        return 0
    ctx = sagemaker_env.from_env(root=a.sm_root)
    try:
        hp = Hyperparameters.from_mapping({**ctx.hyperparameters, **_hp_args(a.hp)})
        report = train(ctx.channel("training"), ctx.model_dir, ctx.output_data_dir, hp)
    except Exception as e:  # noqa: BLE001 - SageMaker wants the reason in output/failure
        ctx.write_failure(f"{type(e).__name__}: {e}")
        print(f"training failed: {e}", file=sys.stderr)
        return 1
    print_report(report)
    return 0


def cmd_score(a) -> int:
    from .score import score

    store = a.store or PROCESSING / "input" / "features"
    model = a.model_dir or PROCESSING / "input" / "model"
    out = a.out or PROCESSING / "output" / "scores"
    r = score(store, model, out, force=a.force, as_of_from=a.as_of_from, as_of_to=a.as_of_to)
    print(
        f"{r['status']}: {r['rows']} scores, model {r['model_version']}, "
        f"features {r['feature_manifest_sha256'][:12]}, scores sha256 {r['scores_sha256'][:12]}"
    )
    return 0


def cmd_demo(a) -> int:
    from .demo import run_demo

    return run_demo(Path(a.workdir), rows=a.rows, seed=a.seed)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="lakehouse.ml", description=__doc__)
    sub = ap.add_subparsers(dest="cmd", required=True)
    t = sub.add_parser("train", help="train, calibrate and evaluate the model")
    t.add_argument("--store", help="feature store root (omit for SageMaker container mode)")
    t.add_argument("--model-dir", default="data/ml/model")
    t.add_argument("--output-dir", default="data/ml/output")
    t.add_argument("--sm-root", default=sagemaker_env.DEFAULT_ROOT)
    t.add_argument("--hp", action="append", metavar="KEY=VALUE", help="hyperparameter override")
    t.set_defaults(fn=cmd_train)
    s = sub.add_parser("score", help="batch-score claim_features with a trained model")
    s.add_argument("--store")
    s.add_argument("--model-dir")
    s.add_argument("--out")
    s.add_argument("--force", action="store_true")
    s.add_argument("--as-of-from")
    s.add_argument("--as-of-to")
    s.set_defaults(fn=cmd_score)
    d = sub.add_parser("demo", help="claimsgen -> features -> train x2 -> score x2 -> report")
    d.add_argument("--workdir", default="data/ml-demo")
    d.add_argument("--rows", type=int, default=20000)
    d.add_argument("--seed", type=int, default=42)
    d.set_defaults(fn=cmd_demo)
    a = ap.parse_args(argv)
    return a.fn(a)


if __name__ == "__main__":
    sys.exit(main())
