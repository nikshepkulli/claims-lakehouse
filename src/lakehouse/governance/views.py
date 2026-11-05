"""Analyst-facing masked views generated from the classification registry (DuckDB dialect).

This mirrors, in DuckDB SQL, what the Snowflake tag-based masking policies do for the
ANALYST role (warehouse/snowflake/90_masking_policies.sql). It exists so the guarantee
"an analyst query cannot return a raw identifier" is verified in CI without a Snowflake
account. It does NOT prove the Snowflake policies behave the same; those are unexecuted.
"""

from __future__ import annotations

from collections.abc import Mapping

from .registry import Column, Table

BUCKET_WIDTH = 5


def _q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def masked_expression(col: Column, sql_type: str) -> str:
    c = _q(col.name)
    t = sql_type.upper()
    if col.handling == "none":
        return c
    if col.handling == "hash":
        return f"sha256(CAST({c} AS VARCHAR))"
    if col.handling == "mask":
        return (
            f"CASE WHEN {c} IS NULL THEN NULL ELSE "
            f"repeat('*', greatest(length({c}) - 2, 0)) || right({c}, 2) END"
        )
    if col.handling == "generalise":
        if "INT" in t:
            return f"({c} - ({c} % {BUCKET_WIDTH}))"
        if t == "DATE":
            return f"CAST(date_trunc('year', {c}) AS DATE)"
        raise ValueError(f"{col.table}.{col.name}: cannot generalise type {sql_type}")
    if col.handling == "drop":
        return f"CAST(NULL AS {sql_type})"
    raise ValueError(f"unknown handling {col.handling!r}")


def analyst_view_sql(
    table: Table, column_types: Mapping[str, str], source: str | None = None
) -> str:
    """CREATE VIEW statement exposing ``table`` as an analyst sees it.

    Raises KeyError if the source has a column the registry does not classify, and
    ValueError if the registry lists a column the source lacks, so the view can never
    silently pass an unclassified column through.
    """
    missing = [c for c in column_types if c not in table.columns]
    if missing:
        raise KeyError(f"{table.name}: unclassified columns {missing}")
    extra = [c for c in table.columns if c not in column_types]
    if extra:
        raise ValueError(f"{table.name}: registry columns missing from source {extra}")
    select = ",\n    ".join(
        f"{masked_expression(table.columns[name], typ)} AS {_q(name)}"
        for name, typ in column_types.items()
    )
    return (
        f"CREATE OR REPLACE VIEW {_q('analyst_' + table.name)} AS\n"
        f"SELECT\n    {select}\nFROM {_q(source or table.name)}"
    )
