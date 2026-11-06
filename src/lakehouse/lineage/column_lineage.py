"""Static column-level lineage for gold, loaded from ``lineage/column_lineage.yaml``.

The mapping is declared by hand and reviewed in a PR (it is documentation of what the Spark code
in ``gold/dims.py`` / ``gold/facts.py`` does), and ``tests/lineage/test_lineage_column_mapping.py``
pins it to the code so it cannot drift: every gold column is mapped to source columns or marked
derived, every source column exists in the silver contract
(``quality/expectations/silver.yaml``), and no stale column remains after a gold schema change.

YAML shape::

    version: 1
    tables:
      fact_claim:
        member_sk:
          sources: [silver.claims.member_id]    # <layer>.<table>.<column>
          transformation: TRANSFORMATION        # IDENTITY | TRANSFORMATION | AGGREGATION
          description: "..."
          masking: true                         # optional
          indirect: [silver.claims.service_date]  # optional: columns that shape the value (joins)
        loaded_at: {derived: "run timestamp"}   # no source column
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from .naming import PRODUCER, table_ref

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_PATH = REPO_ROOT / "lineage" / "column_lineage.yaml"
SUBTYPES = ("IDENTITY", "TRANSFORMATION", "AGGREGATION")
INDIRECT_SUBTYPE = "JOIN"
FACET_URL = (
    "https://openlineage.io/spec/facets/1-2-0/ColumnLineageDatasetFacet.json"
    "#/$defs/ColumnLineageDatasetFacet"
)


class ColumnLineageError(ValueError):
    """The mapping file is malformed."""


@dataclass(frozen=True)
class ColumnMap:
    column: str
    sources: tuple[str, ...] = ()
    transformation: str = "IDENTITY"
    description: str = ""
    masking: bool = False
    indirect: tuple[str, ...] = ()
    derived: str | None = None


@dataclass
class ColumnLineage:
    version: int
    tables: dict[str, dict[str, ColumnMap]] = field(default_factory=dict)

    def all_source_refs(self) -> set[str]:
        return {
            s
            for cols in self.tables.values()
            for m in cols.values()
            for s in (*m.sources, *m.indirect)
        }


def split_source(ref: str) -> tuple[str, str, str]:
    """``silver.claims.member_id`` -> (layer, table, column)."""
    parts = ref.split(".")
    if len(parts) != 3 or not all(parts):
        raise ColumnLineageError(f"source {ref!r} must be <layer>.<table>.<column>")
    return parts[0], parts[1], parts[2]


def load(path: str | Path | None = None) -> ColumnLineage:
    raw = yaml.safe_load(Path(path or DEFAULT_PATH).read_text(encoding="utf-8")) or {}
    if raw.get("version") != 1 or not isinstance(raw.get("tables"), dict):
        raise ColumnLineageError("column lineage file needs `version: 1` and a `tables:` mapping")
    out = ColumnLineage(version=1)
    for table, cols in raw["tables"].items():
        out.tables[table] = {}
        for col, spec in (cols or {}).items():
            spec = spec or {}
            unknown = set(spec) - {
                "sources",
                "transformation",
                "description",
                "masking",
                "indirect",
                "derived",
            }
            if unknown:
                raise ColumnLineageError(f"{table}.{col}: unknown keys {sorted(unknown)}")
            derived = spec.get("derived")
            sources = tuple(spec.get("sources") or ())
            if bool(derived) == bool(sources):
                raise ColumnLineageError(
                    f"{table}.{col}: give exactly one of `sources` or `derived`"
                )
            sub = spec.get("transformation", "IDENTITY")
            if sub not in SUBTYPES:
                raise ColumnLineageError(f"{table}.{col}: transformation must be one of {SUBTYPES}")
            for s in (*sources, *(spec.get("indirect") or ())):
                split_source(s)
            out.tables[table][col] = ColumnMap(
                column=col,
                sources=sources,
                transformation=sub,
                description=str(spec.get("description") or derived or ""),
                masking=bool(spec.get("masking", False)),
                indirect=tuple(spec.get("indirect") or ()),
                derived=str(derived) if derived else None,
            )
    return out


def facet(
    table: str, layer_roots: dict[str, str], mapping: ColumnLineage | None = None
) -> dict[str, Any] | None:
    """The OpenLineage ``columnLineage`` facet of one gold table, or None if it is not mapped
    or has no source column at all.

    ``layer_roots`` maps a layer name used in the YAML (``silver``) to its root URI so source
    columns resolve to the same dataset identity the job reports as inputs. Derived columns have
    no input field and are therefore absent from the facet (OpenLineage cannot say "no source").
    """
    mapping = mapping or load()
    cols = mapping.tables.get(table)
    if cols is None:
        return None

    def field_of(ref: str, kind: str, m: ColumnMap) -> dict[str, Any]:
        layer, tbl, col = split_source(ref)
        if layer not in layer_roots:
            raise ColumnLineageError(f"no root URI for layer {layer!r} (needed by {ref})")
        d = table_ref(layer_roots[layer], tbl)
        tr: dict[str, Any] = {
            "type": kind,
            "subtype": m.transformation if kind == "DIRECT" else INDIRECT_SUBTYPE,
        }
        if m.description:
            tr["description"] = m.description
        if m.masking:
            tr["masking"] = True
        return {"namespace": d.namespace, "name": d.name, "field": col, "transformations": [tr]}

    fields: dict[str, Any] = {}
    for col, m in cols.items():
        if m.derived:
            continue
        inputs = [field_of(s, "DIRECT", m) for s in m.sources]
        inputs += [field_of(s, "INDIRECT", m) for s in m.indirect if s not in m.sources]
        fields[col] = {"inputFields": inputs}
    if not fields:  # every column derived (dim_date): nothing to say
        return None
    return {"_producer": PRODUCER, "_schemaURL": FACET_URL, "fields": fields}


def problems(mapping: ColumnLineage | None = None) -> list[str]:
    """Drift between the mapping and the code/contracts it documents (empty list = in sync).

    * the mapped tables are exactly the gold star-schema tables;
    * each table maps exactly the columns of ``lakehouse.gold.schema`` (none missing, none stale);
    * every source column exists in the silver contract (``quality/expectations/silver.yaml``) or,
      for a ``gold.`` source, in the gold schema.
    """
    from lakehouse.gold import schema as G

    from .schemas import _suite_columns

    mapping = mapping or load()
    out: list[str] = []
    for t in sorted(set(G.TABLES) - set(mapping.tables)):
        out.append(f"gold table {t} has no column lineage")
    for t in sorted(set(mapping.tables) - set(G.TABLES)):
        out.append(f"mapping has unknown gold table {t}")
    silver = {t: {c for c, _ in cols} for t, cols in _suite_columns("silver").items()}
    for t, cols in mapping.tables.items():
        if t not in G.TABLES:
            continue
        actual = {c.name for c in G.TABLES[t]}
        out += [f"{t}.{c}: gold column not mapped" for c in sorted(actual - set(cols))]
        out += [f"{t}.{c}: mapped but not a gold column" for c in sorted(set(cols) - actual)]
        for c, m in cols.items():
            for ref in (*m.sources, *m.indirect):
                layer, tbl, col = split_source(ref)
                if layer == "silver":
                    known = silver.get(tbl)
                elif layer == "gold":
                    known = {x.name for x in G.TABLES[tbl]} if tbl in G.TABLES else None
                else:
                    out.append(f"{t}.{c}: source {ref}: layer must be silver or gold")
                    continue
                if known is None or col not in known:
                    out.append(f"{t}.{c}: source {ref} does not exist")
    return out
