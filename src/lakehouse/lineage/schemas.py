"""Where a dataset's ``schema`` facet comes from (no Spark, no Delta log needed).

Order: (1) gold star tables from the in-code contract ``lakehouse.gold.schema``; (2) the
reviewed suite contracts ``quality/expectations/<layer>.yaml`` when the layer is known;
(3) best effort for local data: the first Parquet data file under the table directory, plus
Hive-style ``k=v`` partition directories (Delta/Spark do not store partition columns in the files).
Returns ``[]`` when nothing is known: a missing schema facet is better than a wrong one.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml

Field = tuple[str, str | None]  # (name, type or None)
EXPECTATIONS_DIR = Path(__file__).resolve().parents[3] / "quality" / "expectations"


@lru_cache(maxsize=8)
def _suite_columns(layer: str) -> dict[str, list[Field]]:
    p = EXPECTATIONS_DIR / f"{layer}.yaml"
    if not p.exists():
        return {}
    out: dict[str, list[Field]] = {}
    for t, spec in (yaml.safe_load(p.read_text(encoding="utf-8")) or {}).get("tables", {}).items():
        cols = (spec or {}).get("columns")
        if isinstance(cols, dict):
            out[t] = [(c, ty) for c, ty in cols.items()]
        elif isinstance(cols, list):
            out[t] = [(c, None) for c in cols]
    return out


def gold_fields(table: str) -> list[Field]:
    from lakehouse.gold import schema as G

    cols = G.TABLES.get(table)
    return [(c.name, c.type) for c in cols] if cols else []


def parquet_fields(table_dir: str | Path) -> list[Field]:
    d = Path(table_dir)
    if not d.is_dir():
        return []
    import pyarrow.parquet as pq

    files = sorted(
        f
        for f in d.rglob("*.parquet")
        if "_delta_log" not in f.parts and not f.name.startswith(".")
    )
    if not files:
        return []
    try:
        schema = pq.read_schema(files[0])
    except Exception:  # unreadable file: no facet beats a wrong one
        return []
    fields: list[Field] = [(f.name, str(f.type)) for f in schema]
    seen = {n for n, _ in fields}
    for part in files[0].relative_to(d).parts[:-1]:
        if "=" in part:
            k = part.split("=", 1)[0]
            if k not in seen:
                fields.append((k, "string"))
                seen.add(k)
    return fields


def fields_for(
    table: str, layer: str | None = None, local_dir: str | Path | None = None
) -> list[Field]:
    g = gold_fields(table)
    if g and layer in (None, "gold"):
        return g
    if layer:
        known = _suite_columns(layer).get(table)
        if known:
            return known
    return parquet_fields(local_dir) if local_dir else []


def schema_facet_fields(fields: list[Field]) -> list[dict[str, Any]]:
    out = []
    for i, (n, t) in enumerate(fields, start=1):
        f: dict[str, Any] = {"name": n, "ordinal_position": i}
        if t:
            f["type"] = t
        out.append(f)
    return out
