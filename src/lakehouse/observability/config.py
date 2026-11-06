"""Observability config loader (``config/observability.yaml``), with validated defaults."""

from __future__ import annotations

import copy
import os
from pathlib import Path
from typing import Any

import yaml

DEFAULTS: dict[str, Any] = {
    "ledger_root": "data/ops",
    "expected_tables": [],
    "defaults": {
        "freshness": {"warn_after_hours": 26.0, "critical_after_hours": 50.0},
        "volume": {
            "window": 14,
            "min_history": 5,
            "z_warn": 3.0,
            "z_crit": 4.5,
            "pct_warn": 30.0,
            "pct_crit": 60.0,
        },
    },
    "tables": {},
    "sinks": {
        "log": {"enabled": True},
        "file": {"enabled": True, "path": "data/ops/alerts.jsonl"},
        "webhook": {
            "enabled": False,
            "url_env": "LAKEHOUSE_ALERT_WEBHOOK_URL",
            "timeout_s": 5,
            "min_severity": "warning",
        },
    },
}


def _merge(base: dict[str, Any], over: dict[str, Any]) -> dict[str, Any]:
    out = copy.deepcopy(base)
    for k, v in (over or {}).items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _merge(out[k], v)
        else:
            out[k] = v
    return out


def _validate(cfg: dict[str, Any]) -> None:
    for scope, block in [("defaults", cfg["defaults"]), *cfg["tables"].items()]:
        for group in ("freshness", "volume"):
            for k, v in block.get(group, {}).items():
                # PyYAML reads `1.0e9` (no sign after e) as a string; fail loudly, not at compare
                if isinstance(v, bool) or not isinstance(v, int | float):
                    raise ValueError(f"{scope}.{group}.{k} must be a number, got {v!r}")
    for scope, block in [("defaults", cfg["defaults"]), *cfg["tables"].items()]:
        f, v = block.get("freshness", {}), block.get("volume", {})
        if f and f["warn_after_hours"] > f["critical_after_hours"]:
            raise ValueError(f"{scope}: freshness warn_after_hours must be <= critical_after_hours")
        if v and (v["z_warn"] > v["z_crit"] or v["pct_warn"] > v["pct_crit"]):
            raise ValueError(f"{scope}: volume warn thresholds must be <= critical thresholds")
        if v and v["min_history"] < 2:
            raise ValueError(f"{scope}: volume min_history must be >= 2 (stddev needs 2 points)")


def load_config(path: str | os.PathLike[str] | None = None) -> dict[str, Any]:
    raw: dict[str, Any] = {}
    if path is not None and Path(path).exists():
        raw = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    cfg = _merge(DEFAULTS, raw)
    # per-table overrides inherit the defaults so thresholds are always complete
    cfg["tables"] = {t: _merge(cfg["defaults"], o or {}) for t, o in cfg["tables"].items()}
    _validate(cfg)
    return cfg


def table_rules(cfg: dict[str, Any], table: str) -> dict[str, Any]:
    return cfg["tables"].get(table, cfg["defaults"])
