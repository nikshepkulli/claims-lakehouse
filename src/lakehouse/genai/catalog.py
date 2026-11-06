"""What the model may see and the SQL may touch, derived from governance/data_classification.yaml.

The registry is the single source of truth. A column is *exposed* to a role only if the role's
policy admits it; everything else is invisible to the prompt, the guardrails and the executor,
so a forbidden column cannot leak through any one of them failing.

Policy (deliberately simple and conservative):
  * identifier == direct            -> never exposed, any role (member_id, provider_id)
  * handling in hash/mask/generalise/drop -> never exposed raw; this feature does not serve the
    masked variants, so birth_year, enrollment_date, record_hash, ... are simply absent
  * role ``public``  : classification PUBLIC or INTERNAL with identifier none
  * role ``analyst`` : also CONFIDENTIAL, and quasi-identifiers kept with handling none
                       (surrogate keys for joins, day-grain date keys, state, sex)
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml

from lakehouse.governance import registry

ROLES = ("public", "analyst")
_RAW_BLOCKED_HANDLING = {"hash", "mask", "generalise", "drop"}
_PUBLIC_CLASSES = {"PUBLIC", "INTERNAL"}
_ANALYST_CLASSES = _PUBLIC_CLASSES | {"CONFIDENTIAL", "PHI_LIKE"}

SEMANTIC_YAML = registry.REPO_ROOT / "dbt" / "models" / "semantic" / "_semantic_layer.yml"


def column_denial(col: registry.Column, role: str) -> str | None:
    """Reason the column is not exposed to ``role``, or None when it is allowed."""
    if role not in ROLES:
        return f"unknown role {role!r}"
    if col.identifier == "direct":
        return f"{col.classification} direct identifier"
    if col.handling in _RAW_BLOCKED_HANDLING:
        return (
            f"{col.classification} column that policy masks ({col.handling}); raw form not served"
        )
    if role == "public":
        if col.classification not in _PUBLIC_CLASSES or col.identifier != "none":
            return f"{col.classification}/{col.identifier} not available to role public"
        return None
    if col.classification not in _ANALYST_CLASSES:  # pragma: no cover - registry enum is closed
        return f"{col.classification} not available to role analyst"
    return None


@dataclass(frozen=True)
class TableView:
    name: str
    layer: str
    allowed: tuple[str, ...]
    denied: dict[str, str]  # column -> reason (kept for explainable rejections, never prompted)


@dataclass(frozen=True)
class Catalog:
    role: str
    tables: dict[str, TableView]
    metrics: tuple[tuple[str, str], ...] = ()

    def table_names(self) -> set[str]:
        return set(self.tables)

    def allowed_columns(self, table: str) -> tuple[str, ...]:
        return self.tables[table].allowed

    def denied_column_names(self) -> dict[str, str]:
        """column name -> reason, over every exposed table (for diagnostics)."""
        out: dict[str, str] = {}
        for t in self.tables.values():
            for c, why in t.denied.items():
                out.setdefault(c, f"{t.name}.{c}: {why}")
        return out


def _metrics(path: Path | None) -> tuple[tuple[str, str], ...]:
    path = path or SEMANTIC_YAML
    if not path.exists():
        return ()
    doc = yaml.safe_load(path.read_text()) or {}
    return tuple(
        (m["name"], (m.get("description") or m.get("label") or "").strip())
        for m in doc.get("metrics", [])
    )


def build_catalog(
    role: str,
    layers: tuple[str, ...] = ("gold",),
    reg: dict[str, registry.Table] | None = None,
    semantic_path: Path | None = None,
) -> Catalog:
    """Catalog for ``role`` over the registry tables in ``layers``.

    A table with no exposed column is dropped entirely. ``layers`` defaults to gold only (the
    Parquet export); add ``mart`` when a DuckDB file with the dbt marts is supplied.
    """
    if role not in ROLES:
        raise ValueError(f"unknown role {role!r}; expected one of {ROLES}")
    reg = reg if reg is not None else registry.load()
    tables: dict[str, TableView] = {}
    for name, t in reg.items():
        if t.layer not in layers:
            continue
        allowed, denied = [], {}
        for cname, col in t.columns.items():
            why = column_denial(col, role)
            if why is None:
                allowed.append(cname)
            else:
                denied[cname] = why
        if allowed:
            tables[name] = TableView(name, t.layer, tuple(allowed), denied)
    metrics = _metrics(semantic_path) if any(t.layer == "mart" for t in tables.values()) else ()
    return Catalog(role, tables, metrics)
