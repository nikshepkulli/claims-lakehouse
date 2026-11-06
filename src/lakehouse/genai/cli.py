"""CLI: ``python -m lakehouse.genai ask "..."`` and ``... eval``.

Providers: ``fake`` (default, offline; answers only the scripted golden questions) and
``bedrock`` (needs AWS credentials in the environment, AWS_REGION, and model access; never run
by this repository's tests or CI).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from .audit import AuditLog
from .catalog import ROLES, build_catalog
from .config import GenAIConfig
from .evaluation import NOT_MEASURED, evaluate, scripted_llm
from .executor import Executor
from .llm import BedrockLLM
from .service import GovernedQA


def _common(p: argparse.ArgumentParser) -> None:
    p.add_argument(
        "--gold-dir",
        default=os.environ.get("LAKEHOUSE_GOLD_EXPORT", "data/gold-delta/export"),
        help="gold Parquet export, <dir>/<table>/*.parquet (Spark job: data/gold-delta/export)",
    )
    p.add_argument("--role", choices=ROLES, default="analyst")
    p.add_argument("--audit-log", default="data/genai/audit.jsonl")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m lakehouse.genai", description=__doc__)
    sub = ap.add_subparsers(dest="cmd", required=True)
    ask = sub.add_parser("ask", help="ask one question")
    ask.add_argument("question")
    ask.add_argument("--provider", choices=("fake", "bedrock"), default="fake")
    ask.add_argument("--json", action="store_true", help="machine-readable output")
    _common(ask)
    ev = sub.add_parser("eval", help="golden + injection evaluation with scripted FakeLLM")
    _common(ev)
    args = ap.parse_args(argv)

    gold = Path(args.gold_dir)
    audit = AuditLog(args.audit_log)
    if args.cmd == "eval":
        reports = evaluate(gold, args.role, audit)
        for r in reports:
            print(r.format())
        print(NOT_MEASURED)
        return 0 if all(r.passed == r.total for r in reports) else 1

    cfg = GenAIConfig.from_env(role=args.role)
    llm = (
        scripted_llm()
        if args.provider == "fake"
        else BedrockLLM(cfg.model_id, cfg.max_output_tokens)
    )
    catalog = build_catalog(args.role)
    executor = Executor(catalog, gold)
    try:
        ans = GovernedQA(llm, executor, catalog, cfg, audit).ask(args.question)
    finally:
        executor.close()
    if args.json:
        print(
            json.dumps(
                {
                    "status": ans.status,
                    "stage": ans.stage,
                    "reasons": ans.reasons,
                    "sql": ans.sql,
                    "columns": ans.columns,
                    "rows": ans.rows,
                },
                default=str,
            )
        )
    else:
        print(f"status: {ans.status} ({ans.stage})")
        for r in ans.reasons:
            print(f"  reason: {r}")
        if ans.sql:
            print(f"sql: {ans.sql}")
        if ans.ok:
            print(" | ".join(ans.columns))
            for row in ans.rows:
                print(" | ".join(str(v) for v in row))
            if ans.truncated:
                print(f"(truncated at {cfg.max_rows} rows)")
    return 0 if ans.ok else 2


if __name__ == "__main__":
    sys.exit(main())
