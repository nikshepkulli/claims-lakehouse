"""Alert hooks: a structured JSON log line always, an optional webhook POST on top.

``CLAIMS_ALERT_WEBHOOK_URL`` (Slack-compatible incoming webhook or any HTTP endpoint accepting
JSON) is read at call time. No URL means log-only. An alert must never mask the real failure, so
every error in here is swallowed and logged. The payload carries ids, dates and the exception
class and message only, no data values.
"""

from __future__ import annotations

import json
import logging
import os
import urllib.request
from typing import Any

log = logging.getLogger("claims_dags.alerts")


def _s(v: Any) -> str | None:
    return None if v is None else str(v)


def build_alert(kind: str, context: dict[str, Any]) -> dict[str, Any]:
    """Flatten an Airflow context into a small, JSON-safe alert record."""
    ti = context.get("task_instance")
    dag = context.get("dag")
    exc = context.get("exception")
    return {
        "event": f"claims_pipeline_{kind}",
        "dag_id": _s(getattr(dag, "dag_id", None) or context.get("dag_id")),
        "task_id": _s(getattr(ti, "task_id", None) or context.get("task_id")),
        "logical_date": _s(context.get("ds")),
        "run_id": _s(context.get("run_id")),
        "try_number": getattr(ti, "try_number", None),
        "max_tries": getattr(ti, "max_tries", None),
        "exception": None if exc is None else f"{type(exc).__name__}: {exc}",
        "log_url": _s(getattr(ti, "log_url", None)),
    }


def emit(alert: dict[str, Any], url: str | None = None) -> None:
    """Log the alert as one JSON line, then POST it when a webhook URL is configured."""
    line = json.dumps(alert, sort_keys=True, default=str)
    log.error("ALERT %s", line)
    url = url or os.environ.get("CLAIMS_ALERT_WEBHOOK_URL")
    if not url:
        return
    body = json.dumps({"text": f"[claims-lakehouse] {line}", **alert}, default=str).encode()
    req = urllib.request.Request(  # noqa: S310 - scheme checked below
        url, data=body, headers={"Content-Type": "application/json"}, method="POST"
    )
    try:
        if not url.startswith(("https://", "http://")):
            raise ValueError("webhook URL must be http(s)")
        urllib.request.urlopen(req, timeout=5).close()  # noqa: S310
    except Exception as e:
        log.warning("alert webhook failed: %s", e)


def on_failure(context: dict[str, Any]) -> None:
    """Task ``on_failure_callback``: fires after the last retry is exhausted."""
    emit(build_alert("task_failed", context))


def on_sla_miss(dag, task_list, blocking_task_list, slas, blocking_tis) -> None:
    """DAG ``sla_miss_callback`` (Airflow 2.x signature)."""
    for sla in slas or []:
        emit(
            {
                "event": "claims_pipeline_sla_miss",
                "dag_id": _s(getattr(dag, "dag_id", None)),
                "task_id": _s(getattr(sla, "task_id", None)),
                "logical_date": _s(getattr(sla, "execution_date", None)),
                "late_tasks": _s(task_list),
                "blocking_tasks": _s(blocking_task_list),
            }
        )
