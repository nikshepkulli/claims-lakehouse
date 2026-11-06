"""Per-micro-batch data-quality gates (ticket #13). Pure Python: no PySpark import.

The streaming sink computes a :class:`BatchStats` for each micro-batch (counts only), this module
turns it into rates, compares them with the thresholds from ``config/streaming_gates.yaml`` and
returns a :class:`GateReport` (pass / warn / fail per gate and overall). The sink then writes the
report to Delta, logs it as structured JSON, and applies the configured ``on_fail`` action.

Rate definitions (the denominator matters; they are repeated in docs/streaming-spark.md):

=========================  ==========================================================
``contract_failure_rate``  contract_violation rows / all rows in the batch
``dlq_rate``               all dead-lettered rows (malformed, null, contract) / all rows
``late_rate``              late events (beyond the watermark, not already accepted) / valid rows
``duplicate_rate``         duplicate valid rows (in-batch or already accepted) / valid rows
``null_rate``              per field: NULL after JSON parsing / parseable rows
``watermark_lag_seconds``  processing time minus the watermark (high-water mark - delay), seconds
=========================  ==========================================================

A gate breaches when its value is strictly greater than the threshold, so ``fail: 0`` means "any
occurrence fails". A threshold of ``null`` disables that level; a gate that is absent from the
YAML is not evaluated. Rate gates are skipped (status ``skipped``) for batches smaller than
``min_batch_rows`` so one bad row in a 3-row batch does not trip a percentage.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

DEFAULT_GATES_PATH = Path(__file__).resolve().parents[3] / "config" / "streaming_gates.yaml"

PASS, WARN, FAIL, SKIPPED = "pass", "warn", "fail", "skipped"
_ORDER = {SKIPPED: -1, PASS: 0, WARN: 1, FAIL: 2}

ON_FAIL_HALT = "halt"
ON_FAIL_DIVERT = "divert"
ON_FAIL_ACTIONS = (ON_FAIL_HALT, ON_FAIL_DIVERT)

RATE_GATES = ("contract_failure_rate", "dlq_rate", "late_rate", "duplicate_rate")
LAG_GATE = "watermark_lag_seconds"
NULL_GATE = "null_rate"
EVENT_FIELDS = (
    "event_id",
    "event_type",
    "event_ts",
    "claim_id",
    "member_id",
    "provider_id",
    "claim_type",
    "amount",
)
FAILURE_MARKER = "DQ_GATE_FAILED"


class GateConfigError(ValueError):
    """The gates YAML is invalid. The message lists every problem, not just the first."""


class GateFailure(RuntimeError):
    """Raised from ``foreachBatch`` when a gate fails and ``on_fail`` is ``halt``."""

    def __init__(self, message: str, report: GateReport | None = None) -> None:
        super().__init__(message)
        self.report = report


@dataclass(frozen=True)
class Threshold:
    warn: float | None = None
    fail: float | None = None

    def status(self, value: float) -> str:
        if self.fail is not None and value > self.fail:
            return FAIL
        if self.warn is not None and value > self.warn:
            return WARN
        return PASS


@dataclass(frozen=True)
class GateConfig:
    on_fail: str = ON_FAIL_HALT
    min_batch_rows: int = 1
    rates: dict[str, Threshold] = field(default_factory=dict)  # RATE_GATES members
    watermark_lag_seconds: Threshold | None = None
    null_rate: dict[str, Threshold] = field(default_factory=dict)  # per event field

    def describe(self) -> dict[str, Any]:
        """Compact, loggable view (printed once at startup)."""

        def t(x: Threshold) -> list[float | None]:
            return [x.warn, x.fail]

        out: dict[str, Any] = {
            "on_fail": self.on_fail,
            "min_batch_rows": self.min_batch_rows,
            **{k: t(v) for k, v in self.rates.items()},
        }
        if self.watermark_lag_seconds:
            out[LAG_GATE] = t(self.watermark_lag_seconds)
        out.update({f"null_rate:{k}": t(v) for k, v in self.null_rate.items()})
        return out


# ---------------------------------------------------------------- loading + validation


def _threshold(name: str, raw: Any, hi: float | None, errors: list[str]) -> Threshold | None:
    if not isinstance(raw, dict) or not set(raw) <= {"warn", "fail"} or not raw:
        errors.append(f"{name}: expected a mapping with 'warn' and/or 'fail', got {raw!r}")
        return None
    vals: dict[str, float | None] = {}
    n_before = len(errors)
    for level in ("warn", "fail"):
        v = raw.get(level)
        if v is None:
            vals[level] = None
            continue
        if isinstance(v, bool) or not isinstance(v, int | float):
            errors.append(f"{name}.{level}: must be a number or null, got {v!r}")
            continue
        if v < 0 or (hi is not None and v > hi):
            rng = f"[0, {hi:g}]" if hi is not None else ">= 0"
            errors.append(f"{name}.{level}: {v} is outside {rng}")
            continue
        vals[level] = float(v)
    w, f = vals.get("warn"), vals.get("fail")
    if w is not None and f is not None and w > f:
        errors.append(f"{name}: warn ({w:g}) must not exceed fail ({f:g})")
    if w is None and f is None and len(errors) == n_before:
        errors.append(f"{name}: set at least one of warn / fail")
    return Threshold(w, f)


def parse_gate_config(doc: Any) -> GateConfig:
    """Validate a parsed YAML document. Raises :class:`GateConfigError` listing all problems."""
    errors: list[str] = []
    if not isinstance(doc, dict):
        raise GateConfigError("gates config must be a mapping")
    for k in set(doc) - {"version", "on_fail", "min_batch_rows", "gates"}:
        errors.append(f"unknown top-level key {k!r}")
    if doc.get("version", 1) != 1:
        errors.append(f"unsupported version {doc.get('version')!r} (this build reads version 1)")
    on_fail = doc.get("on_fail", ON_FAIL_HALT)
    if on_fail not in ON_FAIL_ACTIONS:
        errors.append(f"on_fail must be one of {list(ON_FAIL_ACTIONS)}, got {on_fail!r}")
    mbr = doc.get("min_batch_rows", 1)
    if isinstance(mbr, bool) or not isinstance(mbr, int) or mbr < 1:
        errors.append(f"min_batch_rows must be an integer >= 1, got {mbr!r}")
    gates = doc.get("gates")
    if not isinstance(gates, dict) or not gates:
        errors.append("'gates' must be a non-empty mapping")
        gates = {}

    rates: dict[str, Threshold] = {}
    lag: Threshold | None = None
    nulls: dict[str, Threshold] = {}
    for name, raw in gates.items():
        if name in RATE_GATES:
            if (t := _threshold(name, raw, 1.0, errors)) is not None:
                rates[name] = t
        elif name == LAG_GATE:
            lag = _threshold(name, raw, None, errors)
        elif name == NULL_GATE:
            fields = raw.get("fields") if isinstance(raw, dict) else None
            if not isinstance(fields, dict) or not fields or set(raw) != {"fields"}:
                errors.append("null_rate: expected {fields: {<event field>: {warn, fail}}}")
                continue
            for fname, fraw in fields.items():
                if fname not in EVENT_FIELDS:
                    errors.append(f"null_rate.fields: unknown event field {fname!r}")
                elif (t := _threshold(f"null_rate.{fname}", fraw, 1.0, errors)) is not None:
                    nulls[fname] = t
        else:
            errors.append(f"unknown gate {name!r}; known: {[*RATE_GATES, LAG_GATE, NULL_GATE]}")
    if errors:
        raise GateConfigError("invalid gates config:\n  - " + "\n  - ".join(errors))
    return GateConfig(
        on_fail=on_fail,
        min_batch_rows=mbr,
        rates=rates,
        watermark_lag_seconds=lag,
        null_rate=nulls,
    )


def load_gate_config(path: str | Path | None = None) -> GateConfig:
    """Read and validate the gates YAML (default ``config/streaming_gates.yaml``)."""
    p = Path(path) if path else DEFAULT_GATES_PATH
    try:
        text = p.read_text(encoding="utf-8")
    except OSError as e:
        raise GateConfigError(f"cannot read gates config {p}: {e}") from e
    try:
        doc = yaml.safe_load(text)
    except yaml.YAMLError as e:
        raise GateConfigError(f"gates config {p} is not valid YAML: {e}") from e
    try:
        return parse_gate_config(doc)
    except GateConfigError as e:
        raise GateConfigError(f"{p}: {e}") from e


# ---------------------------------------------------------------- evaluation


@dataclass(frozen=True)
class BatchStats:
    """Counts for one micro-batch (all non-negative; ``duplicate_rows <= valid_rows``)."""

    total_rows: int
    valid_rows: int = 0  # passed the contract (before dedupe)
    parsed_rows: int = 0  # JSON parsed (valid + contract violations); denominator for null rates
    contract_failures: int = 0  # error_type == contract_violation
    dlq_rows: int = 0  # every dead-lettered row
    late_rows: int = 0
    duplicate_rows: int = 0
    null_counts: dict[str, int] = field(default_factory=dict)
    watermark_lag_seconds: float | None = None


@dataclass(frozen=True)
class GateResult:
    gate: str  # e.g. "late_rate" or "null_rate:amount"
    value: float | None
    numerator: int | None
    denominator: int | None
    warn: float | None
    fail: float | None
    status: str


@dataclass(frozen=True)
class GateReport:
    query: str
    batch_id: int
    on_fail: str
    stats: BatchStats
    results: tuple[GateResult, ...]

    @property
    def status(self) -> str:
        worst = max((r.status for r in self.results), key=_ORDER.get, default=PASS)
        return PASS if worst == SKIPPED else worst

    @property
    def failed(self) -> list[GateResult]:
        return [r for r in self.results if r.status == FAIL]

    @property
    def warned(self) -> list[GateResult]:
        return [r for r in self.results if r.status == WARN]

    @property
    def action(self) -> str:
        """What the sink does: ``none`` unless a gate failed, then ``on_fail``."""
        return self.on_fail if self.status == FAIL else "none"

    def breach_summary(self) -> str:
        parts = []
        for r in [*self.failed, *self.warned]:
            limit = r.fail if r.status == FAIL else r.warn
            parts.append(f"{r.gate}={_fmt(r.value)} > {r.status} {_fmt(limit)}")
        return "; ".join(parts)

    def failure_message(self) -> str:
        return (
            f"{FAILURE_MARKER} query={self.query} batch={self.batch_id} "
            f"action={self.on_fail}: {self.breach_summary()}"
        )

    def log_records(self) -> list[tuple[str, str]]:
        """(level, json-line) pairs: one per evaluated gate and a batch summary line."""
        out: list[tuple[str, str]] = []
        for r in self.results:
            if r.status == SKIPPED:
                continue
            doc = {
                "event": "dq_gate",
                "query": self.query,
                "batch_id": self.batch_id,
                "gate": r.gate,
                "status": r.status,
                "value": r.value,
                "warn": r.warn,
                "fail": r.fail,
            }
            out.append((_level(r.status), json.dumps(doc, sort_keys=True)))
        s = self.stats
        summary = {
            "event": "dq_batch",
            "query": self.query,
            "batch_id": self.batch_id,
            "status": self.status,
            "action": self.action,
            "rows": s.total_rows,
            "valid": s.valid_rows,
            "dlq": s.dlq_rows,
            "late": s.late_rows,
            "duplicates": s.duplicate_rows,
            "breaches": self.breach_summary(),
        }
        out.append((_level(self.status), json.dumps(summary, sort_keys=True)))
        return out


def _level(status: str) -> str:
    return {FAIL: "error", WARN: "warning"}.get(status, "info")


def _fmt(x: float | None) -> str:
    return "n/a" if x is None else f"{x:.6g}"


def _ratio(num: int, den: int) -> float:
    return 0.0 if den <= 0 else min(1.0, num / den)


def rates(stats: BatchStats) -> dict[str, tuple[float, int, int]]:
    """gate -> (value, numerator, denominator) for the four batch-level rate gates."""
    return {
        "contract_failure_rate": (
            _ratio(stats.contract_failures, stats.total_rows),
            stats.contract_failures,
            stats.total_rows,
        ),
        "dlq_rate": (_ratio(stats.dlq_rows, stats.total_rows), stats.dlq_rows, stats.total_rows),
        "late_rate": (_ratio(stats.late_rows, stats.valid_rows), stats.late_rows, stats.valid_rows),
        "duplicate_rate": (
            _ratio(stats.duplicate_rows, stats.valid_rows),
            stats.duplicate_rows,
            stats.valid_rows,
        ),
    }


def evaluate(stats: BatchStats, cfg: GateConfig, *, query: str, batch_id: int) -> GateReport:
    """Apply ``cfg`` to ``stats``. Deterministic: the same stats always give the same report."""
    results: list[GateResult] = []
    big_enough = stats.total_rows >= cfg.min_batch_rows
    values = rates(stats)
    for name in RATE_GATES:
        th = cfg.rates.get(name)
        if th is None:
            continue
        v, n, d = values[name]
        results.append(
            GateResult(name, v, n, d, th.warn, th.fail, th.status(v) if big_enough else SKIPPED)
        )
    for fname, th in cfg.null_rate.items():
        n = stats.null_counts.get(fname, 0)
        v = _ratio(n, stats.parsed_rows)
        status = th.status(v) if big_enough and stats.parsed_rows else SKIPPED
        results.append(
            GateResult(f"{NULL_GATE}:{fname}", v, n, stats.parsed_rows, th.warn, th.fail, status)
        )
    lag = cfg.watermark_lag_seconds
    if lag is not None:
        lv = stats.watermark_lag_seconds
        status = SKIPPED if lv is None else lag.status(lv)  # no valid rows -> no watermark
        results.append(GateResult(LAG_GATE, lv, None, None, lag.warn, lag.fail, status))
    return GateReport(query, batch_id, cfg.on_fail, stats, tuple(results))
