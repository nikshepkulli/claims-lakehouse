"""Suite definitions: parse ``quality/expectations/<layer>.yaml`` into plain dataclasses.

File shape (see docs/data-quality.md for the full reference)::

    layer: silver
    tables:
      claims:
        path: claims                  # relative to --root; default = the table key
        optional: false               # true: a missing table is skipped, not a failure
        columns: {claim_id: string}   # or a list of names (presence only); gold uses schema_from
        exact: false                  # true: columns not in the contract fail
        type_severity: blocking
        derived:                      # per-row booleans/numbers computed before validation
          - {name: member_ok, kind: fk, column: member_id, ref: members.member_id}
        expectations:
          - {expectation: expect_column_values_to_be_unique, column: claim_id}
          - {expectation: expect_column_values_to_be_in_set, column: member_ok,
             value_set: [true], severity: warn}

Every key of an expectation except ``expectation``, ``severity`` and ``note`` is a Great
Expectations kwarg. ``severity`` is ``blocking`` (default; a failure exits non-zero) or ``warn``
(reported, exits zero).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

SEVERITIES = ("blocking", "warn")
LAYERS = ("bronze", "silver", "gold")
REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_DIR = REPO_ROOT / "quality" / "expectations"


class SuiteError(ValueError):
    """The suite file is malformed."""


@dataclass(frozen=True)
class ExpSpec:
    type: str
    kwargs: dict[str, Any]
    severity: str = "blocking"
    note: str = ""


@dataclass
class TableSpec:
    name: str
    path: str
    optional: bool = False
    columns: dict[str, str | None] | None = None  # name -> Spark type, None = presence only
    schema_from: str | None = None  # "gold": columns/types/nullability from lakehouse.gold.schema
    exact: bool = False
    type_severity: str = "blocking"
    classified_in: str | None = None  # "gold": every column must be in the classification registry
    derived: list[dict[str, Any]] = field(default_factory=list)
    expectations: list[ExpSpec] = field(default_factory=list)


@dataclass
class LayerSpec:
    layer: str
    description: str
    tables: dict[str, TableSpec]


def _severity(value: Any, where: str) -> str:
    if value not in SEVERITIES:
        raise SuiteError(f"{where}: severity must be one of {SEVERITIES}, got {value!r}")
    return value


def parse(raw: dict[str, Any]) -> LayerSpec:
    layer = raw.get("layer")
    if layer not in LAYERS:
        raise SuiteError(f"layer must be one of {LAYERS}, got {layer!r}")
    tables: dict[str, TableSpec] = {}
    for name, t in (raw.get("tables") or {}).items():
        where = f"{layer}.{name}"
        cols = t.get("columns")
        if isinstance(cols, list):
            cols = dict.fromkeys(cols)
        exps = []
        for i, e in enumerate(t.get("expectations") or []):
            e = dict(e)
            etype = e.pop("expectation", None)
            if not etype or not str(etype).startswith("expect_"):
                raise SuiteError(f"{where}[{i}]: missing/invalid 'expectation'")
            sev = _severity(e.pop("severity", "blocking"), f"{where}[{i}]")
            note = str(e.pop("note", ""))
            exps.append(ExpSpec(etype, e, sev, note))
        for d in t.get("derived") or []:
            if "name" not in d or "kind" not in d:
                raise SuiteError(f"{where}: derived entries need name and kind")
        tables[name] = TableSpec(
            name=name,
            path=t.get("path", name),
            optional=bool(t.get("optional", False)),
            columns=cols,
            schema_from=t.get("schema_from"),
            exact=bool(t.get("exact", False)),
            type_severity=_severity(t.get("type_severity", "blocking"), f"{where}.type_severity"),
            classified_in=t.get("classified_in"),
            derived=list(t.get("derived") or []),
            expectations=exps,
        )
    if not tables:
        raise SuiteError(f"{layer}: no tables")
    return LayerSpec(layer, str(raw.get("description", "")).strip(), tables)


def load_layer(layer: str, directory: Path | str | None = None) -> LayerSpec:
    path = Path(directory or DEFAULT_DIR) / f"{layer}.yaml"
    if not path.is_file():
        raise SuiteError(f"no suite file {path}")
    spec = parse(yaml.safe_load(path.read_text()))
    if spec.layer != layer:
        raise SuiteError(f"{path}: declares layer {spec.layer!r}, expected {layer!r}")
    return spec
