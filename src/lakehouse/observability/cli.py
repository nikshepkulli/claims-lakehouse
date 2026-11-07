"""``python -m lakehouse.observability {wrap,check,ingest-reconciliation,runs}``."""

from __future__ import annotations

import argparse
import json
import logging
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

from .alerts import dispatch
from .checks import evaluate
from .config import load_config
from .ledger import Ledger, utcnow
from .logs import emit, get_logger
from .reconciliation import read_reconciliation, to_run_records
from .schema import LAYERS, SEVERITIES
from .tracker import METRIC_KEYS, dir_size_bytes, track_run

DEFAULT_CONFIG = "config/observability.yaml"
#: env var a wrapped child may write a JSON metrics file to (path passed via this variable)
METRICS_ENV = "LAKEHOUSE_RUN_METRICS_FILE"


def _parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(prog="lakehouse.observability")
    ap.add_argument("--config", default=DEFAULT_CONFIG)
    ap.add_argument("--ops-root", default=None, help="ledger root (default: config ledger_root)")
    ap.add_argument("--log-file", default=None, help="also append JSON-lines logs here")
    sub = ap.add_subparsers(dest="cmd", required=True)

    w = sub.add_parser(
        "wrap", help="run a command and record it in the ledger; exit code passes through"
    )
    w.add_argument("--layer", required=True, choices=LAYERS)
    w.add_argument("--job", required=True)
    w.add_argument("--table", default=None)
    w.add_argument("--batch-id", default=None)
    w.add_argument(
        "--bytes-path", default=None, help="file/dir whose size is recorded as bytes_out"
    )
    for k in METRIC_KEYS:
        if k != "bytes_out":
            w.add_argument(f"--{k.replace('_', '-')}", type=int, default=None)
    w.add_argument("command", nargs=argparse.REMAINDER, help="-- <cmd> [args...]")

    c = sub.add_parser("check", help="evaluate freshness/volume/failure rules and dispatch alerts")
    c.add_argument("--fail-on", choices=[*SEVERITIES, "never"], default="critical")
    c.add_argument("--now", default=None, help="ISO-8601 UTC clock override (tests, backfills)")
    c.add_argument("--no-dispatch", action="store_true", help="print alerts only")

    r = sub.add_parser(
        "ingest-reconciliation", help="load gold_reconciliation rows into the ledger"
    )
    r.add_argument(
        "--table-dir", required=True, help="<gold>/gold_reconciliation (Delta or Parquet)"
    )

    sub.add_parser("runs", help="print ledger rows as JSON lines")
    return ap


def _wrap(args: argparse.Namespace, ledger: Ledger, log: logging.Logger) -> int:
    cmd = args.command[1:] if args.command[:1] == ["--"] else args.command
    if not cmd:
        print("wrap: no command given (use: wrap [opts] -- <cmd>)", file=sys.stderr)
        return 2
    rc = 0
    try:
        with track_run(
            args.layer,
            args.job,
            table=args.table,
            batch_id=args.batch_id,
            ledger=ledger,
            logger=log,
        ) as run:
            env = dict(os.environ, LAKEHOUSE_RUN_ID=run.run_id)
            ledger.path.parent.mkdir(parents=True, exist_ok=True)
            tmp_metrics = Path(ledger.path.parent) / f".metrics-{run.run_id}.json"
            env[METRICS_ENV] = str(tmp_metrics)
            try:
                proc = subprocess.run(cmd, env=env, check=False)  # noqa: S603
            except OSError as e:
                raise RuntimeError(f"cannot execute {cmd[0]!r}: {e}") from e
            rc = proc.returncode
            metrics = {k: getattr(args, k, None) for k in METRIC_KEYS if k != "bytes_out"}
            if tmp_metrics.exists():
                try:
                    reported = json.loads(tmp_metrics.read_text())
                    metrics.update({k: reported[k] for k in METRIC_KEYS if k in reported})
                finally:
                    tmp_metrics.unlink(missing_ok=True)
            if args.bytes_path and Path(args.bytes_path).exists():
                metrics["bytes_out"] = dir_size_bytes(args.bytes_path)
            run.set(**{k: v for k, v in metrics.items() if v is not None})
            if rc != 0:
                raise RuntimeError(f"command exited with code {rc}")
    except RuntimeError:
        return rc or 127
    return rc


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    cfg = load_config(args.config)
    if os.environ.get("LAKEHOUSE_ALERTS_FILE"):  # keep alerts beside a non-default ledger root
        cfg["sinks"]["file"]["path"] = os.environ["LAKEHOUSE_ALERTS_FILE"]
    ledger = Ledger(args.ops_root or cfg["ledger_root"])
    log = get_logger(args.log_file or os.environ.get("LAKEHOUSE_LOG_FILE"))

    if args.cmd == "wrap":
        return _wrap(args, ledger, log)

    if args.cmd == "runs":
        for row in ledger.read():
            print(json.dumps(row, default=lambda v: v.isoformat()))
        return 0

    if args.cmd == "ingest-reconciliation":
        rows, exact = read_reconciliation(args.table_dir)
        if not exact:
            emit(log, "reconciliation_inexact", logging.WARNING, table_dir=args.table_dir)
        recs = to_run_records(rows)
        ledger.append(recs)
        emit(log, "reconciliation_ingested", rows=len(recs), exact=exact)
        return 0

    now = datetime.fromisoformat(args.now) if args.now else utcnow()
    alerts = evaluate(ledger.read(), cfg, now)
    if args.no_dispatch:
        for a in alerts:
            print(json.dumps(a.to_record()))
    else:
        dispatch(alerts, cfg["sinks"], log)
    if args.fail_on == "never":
        return 0
    floor = SEVERITIES.index(args.fail_on)
    return 1 if any(SEVERITIES.index(a.severity) >= floor for a in alerts) else 0
