"""Derived columns: cross-table and aggregate facts turned into per-row booleans.

Great Expectations validates one table at a time. A foreign key, a header-equals-sum-of-lines
identity or an SCD2 history rule involves several rows or tables, so the loader computes it first
as a boolean (or numeric) column and the suite asserts on that column
(``expect_column_values_to_be_in_set: [true]``). A defect therefore shows up as a row count in the
GE result, with sample offending rows, exactly like a plain column rule.

Each builder is ``(frame, spec, ctx) -> Series``; ``ctx`` gives access to the other tables.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import pandas as pd

CENT = 0.005  # amounts are DECIMAL(12,2) loaded as float64: equal when within half a cent


class DerivedError(ValueError):
    """A derived-column spec is malformed or references a missing table or column."""


@dataclass
class Ctx:
    tables: dict[str, pd.DataFrame]  # every table already loaded for the layer
    now: datetime  # naive UTC wall clock used by freshness checks

    def get(self, name: str) -> pd.DataFrame:
        if name not in self.tables:
            raise DerivedError(f"derived column needs table {name!r}, which is not loaded")
        return self.tables[name]


def _col(df: pd.DataFrame, name: str, where: str) -> pd.Series:
    if name not in df.columns:
        raise DerivedError(f"{where}: column {name!r} not found")
    return df[name]


def fk(df: pd.DataFrame, spec: dict[str, Any], ctx: Ctx) -> pd.Series:
    """True when ``column`` resolves to ``ref`` (``table.column``); NULL passes if allow_null."""
    ref_table, ref_col = spec["ref"].split(".", 1)
    parent = _col(ctx.get(ref_table), ref_col, spec["name"])
    col = _col(df, spec["column"], spec["name"])
    ok = col.isin(set(parent.dropna()))
    return ok | col.isna() if spec.get("allow_null") else ok & col.notna()


def eval_(df: pd.DataFrame, spec: dict[str, Any], ctx: Ctx) -> pd.Series:
    """Boolean ``expr`` (pandas ``DataFrame.eval``); rows not matching ``where`` pass."""
    try:
        res = df.eval(spec["expr"], engine="python")
        if "where" in spec:
            res = res | ~df.eval(spec["where"], engine="python")
    except Exception as e:  # undefined column, bad syntax
        raise DerivedError(f"{spec['name']}: {e}") from e
    return res.fillna(False).astype(bool)


def age_hours(df: pd.DataFrame, spec: dict[str, Any], ctx: Ctx) -> pd.Series:
    """Hours between ``column`` and the run time (freshness). Min over rows = newest row's age."""
    ts = pd.to_datetime(_col(df, spec["column"], spec["name"]))
    return (pd.Timestamp(ctx.now) - ts).dt.total_seconds() / 3600.0


def batch_count(df: pd.DataFrame, spec: dict[str, Any], ctx: Ctx) -> pd.Series:
    """Reconciliation re-count: a recorded count equals the rows really present.

    For each row, the table named by ``table_col`` is counted where its ``ref_batch_col`` equals
    the row's ``batch_col`` (and ``ref_filter`` holds, with ``{dataset}`` substituted), then
    compared with ``count_col``. Rows not matching ``where`` pass. A table that is not loaded
    fails the row.
    """
    out = pd.Series(True, index=df.index)
    sel = df.eval(spec["where"], engine="python") if "where" in spec else out
    cache: dict[tuple[str, str], pd.Series] = {}
    for idx in df.index[sel]:
        name = df.at[idx, spec["table_col"]]
        batch = df.at[idx, spec["batch_col"]]
        target = ctx.tables.get(name)
        if target is None:
            out[idx] = False
            continue
        key = (name, spec["ref_batch_col"])
        if key not in cache:
            ref = target
            if "ref_filter" in spec:
                ref = ref[ref.eval(spec["ref_filter"].format(dataset=name), engine="python")]
            cache[key] = ref.groupby(spec["ref_batch_col"]).size()
        out[idx] = int(cache[key].get(batch, 0)) == int(df.at[idx, spec["count_col"]])
    return out


def child_sums(df: pd.DataFrame, spec: dict[str, Any], ctx: Ctx) -> pd.Series:
    """Header = sum of its lines: row count and each ``sums`` pair (header col -> line col)."""
    child = ctx.get(spec["child"])
    key = spec["key"]
    grp = child.groupby(spec["child_key"])
    ok = pd.Series(True, index=df.index)
    if "count" in spec:
        n = df[key].map(grp.size()).fillna(0)
        ok &= n == _col(df, spec["count"], spec["name"])
    for head, line in spec.get("sums", {}).items():
        s = df[key].map(grp[line].sum()).fillna(0.0)
        ok &= (_col(df, head, spec["name"]) - s).abs() <= CENT
    return ok


def scd2(df: pd.DataFrame, spec: dict[str, Any], ctx: Ctx) -> pd.Series:
    """Type-2 history is sound per key: one current version, no gap, no overlap, flag agrees."""
    key, f, t, cur = spec["key"], spec["from_col"], spec["to_col"], spec["current_col"]
    for c in (key, f, t, cur):
        _col(df, c, spec["name"])
    d = df.sort_values([key, f])
    nxt = d.groupby(key)[f].shift(-1)
    chain = (d[t].isna() & nxt.isna()) | (d[t] == nxt)
    window = d[t].isna() | (d[f] < d[t])
    flag = d[cur] == d[t].isna()
    one_current = d.groupby(key)[cur].transform("sum") == 1
    return (chain & window & flag & one_current).reindex(df.index)


BUILDERS: dict[str, Callable[[pd.DataFrame, dict[str, Any], Ctx], pd.Series]] = {
    "fk": fk,
    "eval": eval_,
    "age_hours": age_hours,
    "batch_count": batch_count,
    "child_sums": child_sums,
    "scd2": scd2,
}


def add_derived(df: pd.DataFrame, specs: list[dict[str, Any]], ctx: Ctx) -> pd.DataFrame:
    """Return ``df`` plus one column per spec (``kind`` selects the builder)."""
    out = df.copy()
    for spec in specs:
        kind = spec.get("kind")
        if kind not in BUILDERS:
            raise DerivedError(f"{spec.get('name')}: unknown derived kind {kind!r}")
        out[spec["name"]] = BUILDERS[kind](out, spec, ctx)
    return out
