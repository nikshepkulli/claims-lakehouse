"""Alert record + sinks (log, JSON-lines file, optional webhook)."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import urllib.request
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from .logs import emit, get_logger
from .schema import ALERT_KEYS, SCHEMA_VERSION, SEVERITIES


@dataclass(frozen=True)
class Alert:
    severity: str
    table: str
    rule: str
    observed: float | str | None
    threshold: float | str | None
    message: str
    ts: datetime
    run_id: str | None = None
    #: stable across re-evaluations of the same condition (consumers dedupe on it)
    dedupe_key: str = ""

    def __post_init__(self) -> None:
        if self.severity not in SEVERITIES:
            raise ValueError(f"severity {self.severity!r} not in {SEVERITIES}")

    @property
    def alert_id(self) -> str:
        raw = f"{self.table}|{self.rule}|{self.severity}|{self.dedupe_key}"
        return hashlib.sha1(raw.encode()).hexdigest()[:16]

    def to_record(self) -> dict[str, Any]:
        d = asdict(self)
        d["schema_version"] = SCHEMA_VERSION
        d["alert_id"] = self.alert_id
        d["ts"] = self.ts.isoformat(timespec="seconds")
        return {k: d[k] for k in ALERT_KEYS}


def _rank(sev: str) -> int:
    return SEVERITIES.index(sev)


def dispatch(
    alerts: list[Alert], sinks_cfg: dict[str, Any], logger: logging.Logger | None = None
) -> dict[str, int]:
    """Send alerts to every enabled sink. Returns per-sink delivered counts.

    A sink failure is logged and never raises (alerting must not break the checker).
    """
    log = logger or get_logger()
    sent = {"log": 0, "file": 0, "webhook": 0}
    if not alerts:
        return sent
    records = [a.to_record() for a in alerts]

    if sinks_cfg.get("log", {}).get("enabled", True):
        for r in records:
            lvl = logging.ERROR if r["severity"] == "critical" else logging.WARNING
            emit(log, "alert", lvl, **{k: v for k, v in r.items() if k != "schema_version"})
            sent["log"] += 1

    fcfg = sinks_cfg.get("file", {})
    if fcfg.get("enabled") and fcfg.get("path"):
        try:
            p = Path(fcfg["path"])
            p.parent.mkdir(parents=True, exist_ok=True)
            with p.open("a", encoding="utf-8") as fh:
                for r in records:
                    fh.write(json.dumps(r) + "\n")
            sent["file"] = len(records)
        except OSError as e:
            emit(log, "alert_sink_failed", logging.ERROR, sink="file", error=str(e))

    wcfg = sinks_cfg.get("webhook", {})
    if wcfg.get("enabled"):
        url = os.environ.get(wcfg.get("url_env", ""), "")
        floor = _rank(wcfg.get("min_severity", "warning"))
        wanted = [r for r in records if _rank(r["severity"]) >= floor]
        if not url:
            emit(log, "alert_sink_failed", logging.ERROR, sink="webhook", error="url_env unset")
        elif wanted:
            try:
                req = urllib.request.Request(
                    url,
                    data=json.dumps({"schema_version": SCHEMA_VERSION, "alerts": wanted}).encode(),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with urllib.request.urlopen(req, timeout=wcfg.get("timeout_s", 5)):  # noqa: S310
                    pass
                sent["webhook"] = len(wanted)
            except Exception as e:  # network errors of any kind
                emit(log, "alert_sink_failed", logging.ERROR, sink="webhook", error=str(e))
    return sent
