"""``python -m lakehouse.lineage {wrap,emit,quality,check-mapping}``.

* ``wrap``  run a command and emit START then COMPLETE/FAIL around it; exit code passes through.
* ``emit``  replay the run ledger (#23) as events (post-hoc, idempotent).
* ``quality``  emit a ``quality.<layer>`` run from a #22 result JSON.
* ``check-mapping``  verify ``lineage/column_lineage.yaml`` against the gold schema (CI gate).

Transport failures are logged and counted, never raised; ``wrap`` always returns the child's exit
code. ``emit`` / ``quality`` return 0 too unless ``--fail-on-transport-error`` is given.
"""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from lakehouse.observability.ledger import Ledger, new_run_id
from lakehouse.observability.logs import emit as log_event
from lakehouse.observability.logs import get_logger

from . import column_lineage as CL
from . import dq as DQ
from .events import RunSpec, Stats, build_event
from .naming import NamingError, dataset_ref
from .posthoc import ledger_events, quality_events
from .topology import IO, Roots, layer_io
from .transport import (
    ConsoleTransport,
    Emitter,
    FileTransport,
    HttpTransport,
    Transport,
)

DEFAULT_EVENTS_FILE = "data/lineage/events.jsonl"
FILE_ENV = "LAKEHOUSE_LINEAGE_FILE"
METRICS_ENV = "LAKEHOUSE_RUN_METRICS_FILE"  # same contract as observability wrap
LAYER_PREFIXES = ("bronze=", "silver=", "gold=", "landing=")


def _common(p: argparse.ArgumentParser) -> None:
    p.add_argument(
        "--transport",
        action="append",
        choices=("file", "http", "console"),
        help="repeatable; default: file",
    )
    p.add_argument("--file", default=None, help=f"JSON-lines path (default {DEFAULT_EVENTS_FILE})")
    p.add_argument(
        "--marquez-url", default=None, help="default $MARQUEZ_URL or http://localhost:5002"
    )
    p.add_argument("--validate", action="store_true", help="schema-check events before sending")
    p.add_argument("--log-file", default=None)


def _roots(p: argparse.ArgumentParser) -> None:
    for k in ("landing", "bronze", "silver", "gold"):
        p.add_argument(
            f"--{k}", default=None, help=f"{k} root URI (path, file://, s3a://, abfss://)"
        )


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(prog="lakehouse.lineage", description=__doc__)
    sub = ap.add_subparsers(dest="cmd", required=True)

    w = sub.add_parser("wrap", help="run a command between START and COMPLETE/FAIL events")
    w.add_argument("--job", required=True, help="job name, e.g. bronze, silver_batch")
    w.add_argument("--layer", default=None, help="bronze|silver|gold: use that layer's dataset map")
    w.add_argument("--inputs", nargs="*", default=[], help="[layer=]URI of each input table")
    w.add_argument("--outputs", nargs="*", default=[], help="[layer=]URI of each output table")
    w.add_argument("--rows-out", type=int, default=None)
    w.add_argument("--bytes-out", type=int, default=None)
    w.add_argument("--dq-result", default=None, help="#22 result JSON read after the command")
    w.add_argument("--ops-root", default=None, help="run ledger root to read counts from")
    _roots(w)
    _common(w)
    w.add_argument("command", nargs=argparse.REMAINDER, help="-- <cmd> [args...]")

    e = sub.add_parser("emit", help="replay the run ledger as events")
    e.add_argument("--ops-root", default=None)
    e.add_argument("--run-id", action="append", default=None, help="only these run ids")
    e.add_argument("--since", default=None, help="ISO-8601: only runs that ended at/after")
    e.add_argument("--fail-on-transport-error", action="store_true")
    _roots(e)
    _common(e)

    q = sub.add_parser("quality", help="emit a quality.<layer> run from a #22 result JSON")
    q.add_argument("--result", action="append", required=True)
    q.add_argument("--fail-on-transport-error", action="store_true")
    _roots(q)
    _common(q)

    c = sub.add_parser("check-mapping", help="check column_lineage.yaml against the gold schema")
    c.add_argument("--mapping", default=None)
    return ap


def make_emitter(args: argparse.Namespace, log: Any) -> Emitter:
    names = args.transport or ["file"]
    ts: list[Transport] = []
    for n in dict.fromkeys(names):
        if n == "file":
            ts.append(FileTransport(args.file or os.environ.get(FILE_ENV) or DEFAULT_EVENTS_FILE))
        elif n == "console":
            ts.append(ConsoleTransport())
        else:
            ts.append(HttpTransport(args.marquez_url))
    return Emitter(ts, validate=args.validate, logger=log)


def _roots_of(args: argparse.Namespace) -> Roots:
    return Roots(args.landing, args.bronze, args.silver, args.gold)


def _parse_dataset(spec: str) -> IO:
    layer = ""
    for p in LAYER_PREFIXES:
        if spec.startswith(p):
            layer, spec = p[:-1], spec[len(p) :]
            break
    table = spec.rstrip("/").rsplit("/", 1)[-1]
    if not layer:
        from .topology import GOLD_TABLES

        layer = "gold" if table in GOLD_TABLES else ""
    return IO(dataset_ref(spec), layer, table, spec)


def _wrap_spec(args: argparse.Namespace, run_id: str) -> RunSpec:
    roots = _roots_of(args)
    inputs: list[IO] = []
    outputs: list[IO] = []
    if args.layer and not (args.inputs or args.outputs):
        inputs, outputs = layer_io(args.layer, roots)
    inputs += [_parse_dataset(s) for s in args.inputs]
    outputs += [_parse_dataset(s) for s in args.outputs]
    layer_roots = roots.layer_roots()
    for io in (*inputs, *outputs):  # explicit layer=URI datasets also define that layer's root
        if io.layer in ("bronze", "silver", "gold") and io.layer not in layer_roots:
            layer_roots[io.layer] = io.uri.rstrip("/").rsplit("/", 1)[0]
    return RunSpec(
        run_id=run_id,
        job=args.job,
        inputs=inputs,
        outputs=outputs,
        layer_roots=layer_roots,
        layer=args.layer,
        mapping=CL.load(),
    )


def _collect_stats(spec: RunSpec, args: argparse.Namespace, run_id: str) -> None:
    """Fill output counts from, in order of precedence: flags, the metrics file, the run ledger."""
    run: dict[str, Any] = {}
    mf = os.environ.get(METRICS_ENV)
    if mf and Path(mf).exists():
        # a half-written metrics file is not worth failing over
        with contextlib.suppress(OSError, ValueError):
            run.update(json.loads(Path(mf).read_text()))
    if args.rows_out is not None:
        run["rows_out"] = args.rows_out
    if args.bytes_out is not None:
        run["bytes_out"] = args.bytes_out
    try:
        ledger = Ledger(args.ops_root) if args.ops_root else None
        rows = [r for r in (ledger.read() if ledger else []) if r["run_id"] == run_id]
    except Exception:  # unreadable ledger: stats are optional
        rows = []
    for r in rows:
        for o in spec.outputs:
            if r["table_name"] == o.table:
                st = spec.stats.setdefault(o.table, Stats())
                st.rows = (
                    (st.rows or 0) + (r["rows_out"] or 0) if r["rows_out"] is not None else st.rows
                )
                st.bytes = r["bytes_out"] if r["bytes_out"] is not None else st.bytes
    counted = run.get("rows_out") is not None or run.get("bytes_out") is not None
    if len(spec.outputs) == 1 and spec.outputs[0].table not in spec.stats and counted:
        spec.stats[spec.outputs[0].table] = Stats(run.get("rows_out"), run.get("bytes_out"))
    spec.run_info.update(
        {k: run[k] for k in ("rows_in", "rows_out", "rows_quarantined", "bytes_out") if k in run}
    )


def _attach_dq(spec: RunSpec, path: str) -> None:
    result = DQ.load_result(path)
    for io in spec.inputs:
        t = result["tables"].get(io.table)
        if t:
            f = DQ.table_facets(t)
            if f:
                spec.input_facets[io.table] = f
    spec.run_info["dq_summary"] = DQ.summary(result)


def _summary(em: Emitter, log: Any, run_id: str | None = None) -> None:
    log_event(log, "lineage_summary", run_id=run_id, **em.stats.as_dict())


def _wrap(args: argparse.Namespace, log: Any) -> int:
    cmd = args.command[1:] if args.command[:1] == ["--"] else args.command
    if not cmd:
        print("wrap: no command given (use: wrap [opts] -- <cmd>)", file=sys.stderr)
        return 2
    run_id = os.environ.get("LAKEHOUSE_RUN_ID") or new_run_id()
    env = dict(os.environ, LAKEHOUSE_RUN_ID=run_id)
    try:  # configuration problems disable lineage, they never stop the job
        em = make_emitter(args, log)
        spec = _wrap_spec(args, run_id)
        em.emit(build_event(spec, "START", datetime.now(UTC)))
    except (NamingError, ValueError, OSError, CL.ColumnLineageError) as e:
        log_event(log, "lineage_disabled", 30, run_id=run_id, job=args.job, error=str(e))
        return subprocess.run(cmd, env=env, check=False).returncode  # noqa: S603
    error = None
    try:
        rc = subprocess.run(cmd, env=env, check=False).returncode  # noqa: S603
    except OSError as e:
        rc, error = 127, f"cannot execute {cmd[0]!r}: {e}"
    if rc != 0 and error is None:
        error = f"command exited with code {rc}"
    try:
        if args.dq_result and Path(args.dq_result).exists():
            _attach_dq(spec, args.dq_result)
        _collect_stats(spec, args, run_id)
        kind = "FAIL" if error else "COMPLETE"
        em.emit(build_event(spec, kind, datetime.now(UTC), error=error))
    except Exception as e:
        log_event(log, "lineage_end_event_failed", 30, run_id=run_id, job=args.job, error=str(e))
    _summary(em, log, run_id)
    return rc


def _finish(em: Emitter, args: argparse.Namespace, log: Any) -> int:
    _summary(em, log)
    return 1 if args.fail_on_transport_error and em.stats.failed else 0


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    log = get_logger(getattr(args, "log_file", None) or os.environ.get("LAKEHOUSE_LOG_FILE"))
    if args.cmd == "check-mapping":
        probs = CL.problems(CL.load(args.mapping))
        for p in probs:
            print(p, file=sys.stderr)
        print(f"column lineage: {'OK' if not probs else f'{len(probs)} problem(s)'}")
        return 1 if probs else 0
    if args.cmd == "wrap":
        return _wrap(args, log)
    try:
        em = make_emitter(args, log)
    except ValueError as e:
        print(f"lineage: {e}", file=sys.stderr)
        return 2
    if args.cmd == "emit":
        rows = Ledger(args.ops_root).read()
        if args.run_id:
            rows = [r for r in rows if r["run_id"] in set(args.run_id)]
        if args.since:
            since = datetime.fromisoformat(args.since)
            if since.tzinfo is None:
                since = since.replace(tzinfo=UTC)
            rows = [r for r in rows if r["ended_at"] >= since]
        events = ledger_events(rows, _roots_of(args))
    else:
        events = []
        for path in args.result:
            events += quality_events(DQ.load_result(path), _roots_of(args))
    for ev in events:
        em.emit(ev)
    return _finish(em, args, log)
