"""Freshness and volume checks over ledger rows. Pure functions: no I/O, injectable clock."""

from __future__ import annotations

import math
import statistics
from datetime import datetime
from typing import Any

from .alerts import Alert
from .config import table_rules


def age_hours(last: datetime, now: datetime) -> float:
    return (now - last).total_seconds() / 3600.0


def zscore(observed: float, history: list[float]) -> float | None:
    """Sample z-score of ``observed`` vs ``history``; None when undefined (n<2 or zero spread)."""
    if len(history) < 2:
        return None
    sd = statistics.stdev(history)
    if sd == 0 or math.isnan(sd):
        return None
    return (observed - statistics.fmean(history)) / sd


def pct_drift(observed: float, history: list[float]) -> float | None:
    """Signed % change of ``observed`` vs the history mean; None when the mean is 0/empty."""
    if not history:
        return None
    mean = statistics.fmean(history)
    if mean == 0:
        return None
    return (observed - mean) / mean * 100.0


def _sev(value: float, warn: float, crit: float) -> str | None:
    if value >= crit:
        return "critical"
    if value >= warn:
        return "warning"
    return None


def evaluate(runs: list[dict[str, Any]], cfg: dict[str, Any], now: datetime) -> list[Alert]:
    """Evaluate run_failed, freshness_sla, volume_zscore and volume_drift for every table.

    ``runs`` are ledger rows (dicts). Tables = tables seen in the ledger plus
    ``cfg["expected_tables"]`` (an expected table with no successful run is critical).
    """
    by_table: dict[str, list[dict[str, Any]]] = {}
    for r in sorted(runs, key=lambda r: (r["ended_at"], r["run_id"])):
        by_table.setdefault(r["table_name"], []).append(r)
    for t in cfg.get("expected_tables", []):
        by_table.setdefault(t, [])

    alerts: list[Alert] = []
    for table in sorted(by_table):
        rows = by_table[table]
        rules = table_rules(cfg, table)
        ok = [r for r in rows if r["status"] == "success"]
        last_ok = ok[-1] if ok else None

        # 1. most recent run failed (and nothing succeeded after it)
        if rows and rows[-1]["status"] == "failed":
            bad = rows[-1]
            alerts.append(
                Alert(
                    "critical",
                    table,
                    "run_failed",
                    "failed",
                    "success",
                    f"latest run failed: {bad.get('error') or 'no error text'}",
                    now,
                    bad["run_id"],
                    bad["run_id"],
                )
            )

        # 2. freshness vs SLA
        f = rules["freshness"]
        if last_ok is None:
            alerts.append(
                Alert(
                    "critical",
                    table,
                    "freshness_sla",
                    "never_succeeded",
                    "1 success",
                    "table has no successful run in the ledger",
                    now,
                    None,
                    "never",
                )
            )
        else:
            age = age_hours(last_ok["ended_at"], now)
            sev = _sev(age, f["warn_after_hours"], f["critical_after_hours"])
            if sev:
                thr = f["critical_after_hours"] if sev == "critical" else f["warn_after_hours"]
                alerts.append(
                    Alert(
                        sev,
                        table,
                        "freshness_sla",
                        round(age, 2),
                        thr,
                        f"last success {age:.1f} h ago (SLA {thr} h)",
                        now,
                        last_ok["run_id"],
                        last_ok["ended_at"].isoformat(),
                    )
                )

        # 3. volume vs trailing successful runs
        v = rules["volume"]
        usable = [r for r in ok if r.get("rows_out") is not None]
        if len(usable) >= 2:
            latest = usable[-1]
            hist = [float(r["rows_out"]) for r in usable[:-1]][-v["window"] :]
            if len(hist) >= v["min_history"]:
                obs = float(latest["rows_out"])
                z = zscore(obs, hist)
                if z is not None:
                    sev = _sev(abs(z), v["z_warn"], v["z_crit"])
                    if sev:
                        thr = v["z_crit"] if sev == "critical" else v["z_warn"]
                        alerts.append(
                            Alert(
                                sev,
                                table,
                                "volume_zscore",
                                round(z, 2),
                                thr,
                                f"rows_out={obs:.0f}, z={z:+.2f} vs last {len(hist)} runs",
                                now,
                                latest["run_id"],
                                latest["run_id"],
                            )
                        )
                d = pct_drift(obs, hist)
                if d is not None:
                    sev = _sev(abs(d), v["pct_warn"], v["pct_crit"])
                    if sev:
                        thr = v["pct_crit"] if sev == "critical" else v["pct_warn"]
                        alerts.append(
                            Alert(
                                sev,
                                table,
                                "volume_drift",
                                round(d, 2),
                                thr,
                                f"rows_out={obs:.0f}, {d:+.1f}% vs mean of last {len(hist)} runs",
                                now,
                                latest["run_id"],
                                latest["run_id"],
                            )
                        )
    return alerts
