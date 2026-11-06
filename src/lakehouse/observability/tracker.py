"""``track_run``: opt-in context manager / decorator that records one ledger row per run.

    with track_run("silver", "silver_batch", table="claims", batch_id="2024-01-05") as run:
        ...
        run.set(rows_in=1000, rows_out=990, rows_quarantined=10)

A failure inside the block is recorded (status=failed, error=exception class + message) and
re-raised; observability never swallows a job error. Failures of the ledger/log write itself are
logged and swallowed: a monitoring outage must not fail a data job.
"""

from __future__ import annotations

import functools
import logging
import os
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any, TypeVar

from .ledger import Ledger, RunRecord, new_run_id, utcnow
from .logs import emit, get_logger

F = TypeVar("F", bound=Callable[..., Any])
METRIC_KEYS = ("rows_in", "rows_out", "rows_quarantined", "bytes_out")


def dir_size_bytes(path: str | os.PathLike[str]) -> int:
    p = Path(path)
    if p.is_file():
        return p.stat().st_size
    return sum(f.stat().st_size for f in p.rglob("*") if f.is_file())


class RunHandle:
    def __init__(self, run_id: str, layer: str, job: str):
        self.run_id, self.layer, self.job = run_id, layer, job
        self.metrics: dict[str, int | None] = dict.fromkeys(METRIC_KEYS)
        #: extra per-table rows (table_name -> metrics) for jobs that cover several tables
        self.tables: dict[str, dict[str, int | None]] = {}

    def set(self, **metrics: int | None) -> None:
        for k, v in metrics.items():
            if k not in METRIC_KEYS:
                raise KeyError(f"unknown metric {k!r}; allowed: {METRIC_KEYS}")
            self.metrics[k] = None if v is None else int(v)

    def set_table(self, table: str, **metrics: int | None) -> None:
        bad = [k for k in metrics if k not in METRIC_KEYS]
        if bad:
            raise KeyError(f"unknown metric(s) {bad}; allowed: {METRIC_KEYS}")
        self.tables.setdefault(table, dict.fromkeys(METRIC_KEYS)).update(
            {k: None if v is None else int(v) for k, v in metrics.items()}
        )


@contextmanager
def track_run(
    layer: str,
    job: str,
    *,
    table: str | None = None,
    batch_id: str | None = None,
    ledger: Ledger | None = None,
    logger: logging.Logger | None = None,
    run_id: str | None = None,
) -> Iterator[RunHandle]:
    run_id = run_id or os.environ.get("LAKEHOUSE_RUN_ID") or new_run_id()
    ledger = ledger or Ledger()
    log = logger or get_logger(os.environ.get("LAKEHOUSE_LOG_FILE"))
    handle = RunHandle(run_id, layer, job)
    started = utcnow()
    ctx = {"run_id": run_id, "layer": layer, "job": job}
    emit(log, "run_started", batch_id=batch_id, table=table, **ctx)
    status, error = "success", None
    try:
        yield handle
    except BaseException as exc:  # record then re-raise, including KeyboardInterrupt
        status, error = "failed", f"{type(exc).__name__}: {exc}"[:2000]
        raise
    finally:
        ended = utcnow()
        try:
            if handle.tables:
                recs = [
                    RunRecord(
                        run_id, layer, job, started, ended, status, t, batch_id, **m, error=error
                    )
                    for t, m in handle.tables.items()
                ]
            else:
                recs = [
                    RunRecord(
                        run_id,
                        layer,
                        job,
                        started,
                        ended,
                        status,
                        table or layer,
                        batch_id,
                        **handle.metrics,
                        error=error,
                    )
                ]
            ledger.append(recs)
        except Exception as werr:  # monitoring outage must not fail the job
            emit(log, "ledger_write_failed", logging.ERROR, error=str(werr), **ctx)
        emit(
            log,
            "run_finished",
            logging.INFO if status == "success" else logging.ERROR,
            status=status,
            batch_id=batch_id,
            table=table,
            duration_s=round((ended - started).total_seconds(), 3),
            error=error,
            **handle.metrics,
            **ctx,
        )


def tracked(layer: str, job: str, **kw: Any) -> Callable[[F], F]:
    """Decorator form. The wrapped function may return a dict of metrics to record."""

    def deco(fn: F) -> F:
        @functools.wraps(fn)
        def inner(*a: Any, **k: Any) -> Any:
            with track_run(layer, job, **kw) as run:
                out = fn(*a, **k)
                if isinstance(out, dict):
                    run.set(**{m: out[m] for m in METRIC_KEYS if m in out})
                return out

        return inner  # type: ignore[return-value]

    return deco
