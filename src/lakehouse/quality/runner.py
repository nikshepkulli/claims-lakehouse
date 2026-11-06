"""Run a layer's suites with Great Expectations over pandas frames and build the reports.

Flow: load every table of the layer (``loader``), add derived columns (``derived``), validate each
table with its own GE suite, record severity per expectation (suite metadata), write
``<out>/<layer>-result.json`` and (optionally) the HTML data docs. ``LayerResult.exit_code`` is 1
when a *blocking* expectation failed or a required table is missing.
"""

from __future__ import annotations

import json
import math
import os
import warnings
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from . import derived as D
from . import loader
from .suites import ExpSpec, LayerSpec, TableSpec

SCHEMA_SUFFIX = ":schema"
SAMPLE_LIMIT = 5


@dataclass
class ExpResult:
    type: str
    kwargs: dict[str, Any]
    severity: str
    success: bool
    note: str = ""
    observed_value: Any = None
    element_count: int | None = None
    unexpected_count: int | None = None
    unexpected_percent: float | None = None
    samples: list[Any] = field(default_factory=list)
    error: str | None = None


@dataclass
class TableResult:
    name: str
    path: str
    status: str  # "validated" | "missing" | "skipped"
    rows: int | None = None
    kind: str | None = None
    expectations: list[ExpResult] = field(default_factory=list)
    problem: str | None = None  # set when a required table is missing or could not be prepared


@dataclass
class LayerResult:
    layer: str
    root: str
    generated_at: str
    ge_version: str
    tables: dict[str, TableResult]

    def failures(self, severity: str) -> list[tuple[str, ExpResult | None, str]]:
        out: list[tuple[str, ExpResult | None, str]] = []
        for t in self.tables.values():
            if t.problem and severity == "blocking":
                out.append((t.name, None, t.problem))
            out += [
                (t.name, e, "") for e in t.expectations if not e.success and e.severity == severity
            ]
        return out

    @property
    def exit_code(self) -> int:
        return 1 if self.failures("blocking") else 0

    def summary(self) -> dict[str, int]:
        exps = [e for t in self.tables.values() for e in t.expectations]
        return {
            "tables": sum(t.status == "validated" for t in self.tables.values()),
            "tables_skipped": sum(t.status == "skipped" for t in self.tables.values()),
            "expectations": len(exps),
            "passed": sum(e.success for e in exps),
            "failed_blocking": len(self.failures("blocking")),
            "failed_warn": len(self.failures("warn")),
        }

    def to_json(self) -> dict[str, Any]:
        s = self.summary()
        return _clean(
            {
                "layer": self.layer,
                "root": self.root,
                "generated_at": self.generated_at,
                "great_expectations_version": self.ge_version,
                "success": s["failed_blocking"] == 0,
                "summary": s,
                "tables": {
                    n: {
                        "path": t.path,
                        "status": t.status,
                        "rows": t.rows,
                        "storage": t.kind,
                        "problem": t.problem,
                        "expectations": [e.__dict__ for e in t.expectations],
                    }
                    for n, t in self.tables.items()
                },
            }
        )


def _clean(o: Any) -> Any:
    """JSON-safe copy: numpy scalars, NaN/NaT, timestamps."""
    if isinstance(o, dict):
        return {str(k): _clean(v) for k, v in o.items()}
    if isinstance(o, list | tuple):
        return [_clean(v) for v in o]
    if hasattr(o, "item") and callable(o.item):  # numpy scalar
        try:
            return _clean(o.item())
        except (ValueError, TypeError):
            return str(o)
    if isinstance(o, float) and (math.isnan(o) or math.isinf(o)):
        return None
    if o is None or isinstance(o, str | int | float | bool):
        return o
    return str(o)


# ---------------------------------------------------------------- schema pseudo-table


def _gold_columns(name: str) -> list[Any]:
    from lakehouse.gold import paths as P
    from lakehouse.gold import schema as G

    ops = {P.RECONCILIATION: G.RECONCILIATION_COLS, P.EXCLUSIONS: G.EXCLUSION_COLS}
    cols = G.TABLES.get(name) or ops.get(name)
    if cols is None:
        raise D.DerivedError(f"{name}: not in lakehouse.gold.schema")
    return cols


def _expected(spec: TableSpec) -> dict[str, tuple[str | None, bool]]:
    """column -> (spark type or None = unchecked, nullable)."""
    if spec.schema_from == "gold":
        return {c.name: (c.type, c.nullable) for c in _gold_columns(spec.name)}
    return {c: (t, True) for c, t in (spec.columns or {}).items()}


def _classified(table: str) -> set[str] | None:
    from lakehouse.governance import registry

    t = registry.load().get(table)
    return set(t.columns) if t else None


def schema_frame(spec: TableSpec, actual: dict[str, str]):
    """One row per expected-or-actual column with boolean verdicts the schema suite asserts on."""
    import pandas as pd

    exp = _expected(spec)
    classified = _classified(spec.name) if spec.classified_in else None
    rows = []
    for col in [*exp, *[c for c in actual if c not in exp]]:
        want = exp.get(col, (None, True))[0]
        rows.append(
            {
                "column": col,
                "expected_type": want,
                "actual_type": actual.get(col),
                "present_ok": col in actual or col not in exp,
                "type_ok": want is None or col not in actual or actual[col] == want,
                "no_extra_ok": col in exp or not spec.exact,
                "classified_ok": classified is None or col not in actual or col in classified,
            }
        )
    return pd.DataFrame(rows)


def _schema_expectations(spec: TableSpec) -> list[ExpSpec]:
    def inset(col: str, sev: str, note: str) -> ExpSpec:
        return ExpSpec(
            "expect_column_values_to_be_in_set",
            {"column": col, "value_set": [True]},
            sev,
            note,
        )

    out = [
        inset("present_ok", "blocking", "every contract column is present"),
        inset("type_ok", spec.type_severity, "column types equal the contract"),
    ]
    if spec.exact:
        out.append(inset("no_extra_ok", "blocking", "no column outside the contract"))
    if spec.classified_in:
        out.append(
            inset("classified_ok", "blocking", "every column is in the classification registry")
        )
    return out


def _nullability_expectations(spec: TableSpec) -> list[ExpSpec]:
    if spec.schema_from != "gold":
        return []
    return [
        ExpSpec(
            "expect_column_values_to_not_be_null",
            {"column": c},
            "blocking",
            "NOT NULL in the DDL contract",
        )
        for c, (_, nullable) in _expected(spec).items()
        if not nullable
    ]


# ---------------------------------------------------------------- Great Expectations


class _Docs:
    """Thin wrapper over one file-backed GE context (suites, validation store, data docs)."""

    def __init__(self, out: Path, layer: str):
        import great_expectations as gx

        self.gx = gx
        self.ctx = gx.get_context(mode="file", project_root_dir=str(out))
        self.ctx.variables.progress_bars = {"globally": False}
        self.ds = self.ctx.data_sources.add_or_update_pandas(f"{layer}_frames")
        self.layer = layer

    def validate(self, suite_name: str, exps: list[ExpSpec], frame, meta: dict[str, Any]):
        from great_expectations.expectations.expectation_configuration import (
            ExpectationConfiguration,
        )

        suite = self.gx.ExpectationSuite(name=suite_name, meta=meta)
        for e in exps:
            suite.add_expectation_configuration(
                ExpectationConfiguration(
                    type=e.type,
                    kwargs=e.kwargs,
                    meta={"severity": e.severity, "note": e.note},
                )
            )
        suite = self.ctx.suites.add_or_update(suite)
        try:
            asset = self.ds.get_asset(suite_name)
        except LookupError:
            asset = self.ds.add_dataframe_asset(suite_name)
        try:
            bd = asset.get_batch_definition("whole")
        except KeyError:
            bd = asset.add_batch_definition_whole_dataframe("whole")
        vd = self.ctx.validation_definitions.add_or_update(
            self.gx.ValidationDefinition(name=f"{suite_name}.validation", data=bd, suite=suite)
        )
        return vd.run(batch_parameters={"dataframe": frame}, result_format="SUMMARY")

    def build_docs(self, out: Path) -> Path:
        """Render the HTML data docs to ``<out>/data_docs`` (index.html inside)."""
        target = out / "data_docs"
        site = self.ctx.variables.data_docs_sites["local_site"]
        site["store_backend"]["base_directory"] = str(target)
        self.ctx.variables.data_docs_sites = {"local_site": site}
        self.ctx.build_data_docs()
        return target / "index.html"


def _to_results(vr, specs: list[ExpSpec]) -> list[ExpResult]:
    """Map GE validation results back to our specs (severity travels in expectation meta)."""
    out = []
    for er in vr.results:
        cfg = er.expectation_config
        meta = cfg.meta or {}
        kwargs = {k: v for k, v in cfg.kwargs.items() if k != "batch_id"}
        res = er.result or {}
        err = None
        if er.exception_info:
            infos = [
                i.get("exception_message") or i.get("raised_exception")
                for i in er.exception_info.values()
                if isinstance(i, dict) and i.get("raised_exception")
            ]
            err = "; ".join(str(i) for i in infos) or None
        out.append(
            ExpResult(
                type=cfg.type,
                kwargs=kwargs,
                severity=meta.get("severity", "blocking"),
                success=bool(er.success) and err is None,
                note=meta.get("note", ""),
                observed_value=res.get("observed_value"),
                element_count=res.get("element_count"),
                unexpected_count=res.get("unexpected_count"),
                unexpected_percent=res.get("unexpected_percent"),
                samples=list(res.get("partial_unexpected_list") or [])[:SAMPLE_LIMIT],
                error=err,
            )
        )
    return out


def run_layer(
    spec: LayerSpec,
    root: str,
    out: Path,
    *,
    now: datetime | None = None,
    docs: bool = True,
) -> LayerResult:
    """Validate every table of ``spec`` under ``root``; write ``<out>/<layer>-result.json``."""
    import great_expectations as gx

    # GE's regex metric uses str.contains; our anchored patterns have groups and pandas nags.
    warnings.filterwarnings("ignore", message=".*has match groups.*", category=UserWarning)
    out.mkdir(parents=True, exist_ok=True)
    now = (now or datetime.now(UTC)).replace(tzinfo=None)
    result = LayerResult(
        spec.layer, root, datetime.now(UTC).isoformat(timespec="seconds"), gx.__version__, {}
    )

    loaded: dict[str, loader.Loaded] = {}
    for name, t in spec.tables.items():
        if loader.exists(root, t.path):
            try:
                loaded[name] = loader.load_table(root, t.path)
                continue
            except Exception as e:  # unreadable table is a blocking problem, not a crash
                result.tables[name] = TableResult(
                    name, t.path, "missing", problem=f"unreadable: {e}"
                )
                continue
        if t.optional:
            result.tables[name] = TableResult(name, t.path, "skipped")
        else:
            result.tables[name] = TableResult(
                name, t.path, "missing", problem=f"required table not found at {root}/{t.path}"
            )

    ctx = D.Ctx({n: ld.frame for n, ld in loaded.items()}, now)
    ge = _Docs(out, spec.layer)
    for name, ld in loaded.items():
        t = spec.tables[name]
        tr = TableResult(name, t.path, "validated", rows=len(ld.frame), kind=ld.kind)
        result.tables[name] = tr
        try:
            frame = D.add_derived(ld.frame, t.derived, ctx)
            sframe = schema_frame(t, ld.types)
        except D.DerivedError as e:
            tr.problem = f"suite cannot be evaluated: {e}"
            continue
        meta = {"layer": spec.layer, "table": name, "rows": len(frame)}
        main = _nullability_expectations(t) + t.expectations
        vr = ge.validate(f"{spec.layer}.{name}", main, frame, meta)
        tr.expectations += _to_results(vr, main)
        sch = _schema_expectations(t)
        if sch:
            vr = ge.validate(f"{spec.layer}.{name}{SCHEMA_SUFFIX}", sch, sframe, meta)
            tr.expectations += _to_results(vr, sch)

    (out / f"{spec.layer}-result.json").write_text(
        json.dumps(result.to_json(), indent=2, sort_keys=False) + "\n"
    )
    if docs:
        ge.build_docs(out)
    return result


def report(result: LayerResult) -> str:
    """Human summary for stdout; GitHub Actions annotations when running in Actions."""
    gha = os.environ.get("GITHUB_ACTIONS") == "true"
    s = result.summary()
    lines = [
        f"[dq] layer={result.layer} tables={s['tables']} (skipped {s['tables_skipped']}) "
        f"expectations={s['expectations']} passed={s['passed']} "
        f"blocking_failures={s['failed_blocking']} warnings={s['failed_warn']}"
    ]
    for sev, tag in (("blocking", "FAIL"), ("warn", "WARN")):
        for table, e, problem in result.failures(sev):
            if e is None:
                msg = f"{table}: {problem}"
            else:
                col = e.kwargs.get("column") or e.kwargs.get("column_list") or ""
                what = f"{e.note} | " if e.note else ""
                msg = (
                    f"{table}.{col} {e.type} {what}unexpected={e.unexpected_count} "
                    f"observed={e.observed_value} samples={e.samples}"
                    + (f" error={e.error}" if e.error else "")
                )
            lines.append(f"[dq] {tag} {msg}")
            if gha:
                level = "error" if sev == "blocking" else "warning"
                lines.append(f"::{level} title=data quality ({result.layer})::{msg[:400]}")
    lines.append("[dq] RESULT: " + ("FAILED" if result.exit_code else "ok"))
    return "\n".join(lines)
