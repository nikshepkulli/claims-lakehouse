"""Data-quality facets from the #22 result JSON (``<out>/<layer>-result.json``).

Shape consumed (see ``lakehouse.quality.runner.LayerResult.to_json``)::

    {"layer": "silver", "tables": {"claims": {"status": "validated", "rows": 1000,
       "expectations": [{"type": "expect_column_values_to_be_unique", "kwargs": {"column": ..},
                         "severity": "blocking|warn", "success": true, ...}]}}}

Each validated table becomes an *input* dataset of a ``quality.<layer>`` job run, carrying
``dataQualityAssertions`` (one assertion per expectation) and ``dataQualityMetrics`` (row count,
null counts where a not-null expectation reported them).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .naming import PRODUCER

ASSERTIONS_URL = (
    "https://openlineage.io/spec/facets/1-1-0/DataQualityAssertionsDatasetFacet.json"
    "#/$defs/DataQualityAssertionsDatasetFacet"
)
METRICS_URL = (
    "https://openlineage.io/spec/facets/1-0-3/DataQualityMetricsInputDatasetFacet.json"
    "#/$defs/DataQualityMetricsInputDatasetFacet"
)
SEVERITY = {"blocking": "error", "warn": "warn"}


def load_result(path: str | Path) -> dict[str, Any]:
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(data, dict) or not isinstance(data.get("tables"), dict):
        raise ValueError(f"{path}: not a data-quality result (missing `tables`)")
    return data


#: verdict columns of the synthetic schema-check frame (quality.runner.schema_frame);
#: they are not columns of the dataset
SCHEMA_VERDICTS = ("present_ok", "type_ok", "no_extra_ok", "classified_ok")


def _assertion(e: dict[str, Any]) -> dict[str, Any]:
    kwargs = dict(e.get("kwargs") or {})
    name = str(e["type"]).removeprefix("expect_")
    if kwargs.get("column") in SCHEMA_VERDICTS:
        name, kwargs["column"] = f"schema_{kwargs['column']}", None
    a: dict[str, Any] = {
        "assertion": name,
        "success": bool(e["success"]),
        "severity": SEVERITY.get(e.get("severity", ""), str(e.get("severity", "error"))),
    }
    if kwargs.get("column"):
        a["column"] = str(kwargs["column"])
    if e.get("note"):
        a["description"] = str(e["note"])
    if e.get("observed_value") is not None and not e["success"]:
        a["actual"] = str(e["observed_value"])
    return a


def table_facets(table: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """``inputFacets`` for one validated table of the result JSON (empty when not validated)."""
    if table.get("status") != "validated":
        return {}
    exps = table.get("expectations") or []
    nulls: dict[str, int] = {}
    for e in exps:
        col = (e.get("kwargs") or {}).get("column")
        if e["type"] == "expect_column_values_to_not_be_null" and col:
            nulls[col] = int(e.get("unexpected_count") or 0)
    metrics: dict[str, Any] = {
        "_producer": PRODUCER,
        "_schemaURL": METRICS_URL,
        "columnMetrics": {c: {"nullCount": n} for c, n in nulls.items()},
    }
    if table.get("rows") is not None:
        metrics["rowCount"] = int(table["rows"])
    return {
        "dataQualityAssertions": {
            "_producer": PRODUCER,
            "_schemaURL": ASSERTIONS_URL,
            "assertions": [_assertion(e) for e in exps],
        },
        "dataQualityMetrics": metrics,
    }


def summary(result: dict[str, Any]) -> dict[str, Any]:
    s = result.get("summary") or {}
    return {k: s[k] for k in ("expectations", "passed", "failed_blocking", "failed_warn") if k in s}
