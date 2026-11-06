"""Read a layer table into pandas without Spark.

Delta tables (``_delta_log`` present) are read through ``deltalake`` (delta-rs), which resolves the
transaction log, so files removed by a replaceWhere overwrite are not double counted. A directory of
plain Parquet (for example the gold ``export/``) is read as a Parquet dataset. Both give an Arrow
table; its schema is rendered with Spark ``simpleString`` names so a suite can compare it with the
contract in ``lakehouse.gold.schema``.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd
    import pyarrow as pa


@dataclass
class Loaded:
    frame: pd.DataFrame
    types: dict[str, str]  # column -> Spark simpleString type name
    kind: str  # "delta" | "parquet"


def simple_type(t: pa.DataType) -> str:
    """Arrow type -> Spark ``simpleString`` (what ``lakehouse.gold.schema`` uses)."""
    import pyarrow.types as pt

    if (
        pt.is_string(t)
        or pt.is_large_string(t)
        or getattr(pt, "is_string_view", lambda _: False)(t)
    ):
        return "string"
    if pt.is_boolean(t):
        return "boolean"
    if pt.is_int8(t):
        return "tinyint"
    if pt.is_int16(t):
        return "smallint"
    if pt.is_int32(t):
        return "int"
    if pt.is_int64(t):
        return "bigint"
    if pt.is_float32(t):
        return "float"
    if pt.is_float64(t):
        return "double"
    if pt.is_decimal(t):
        return f"decimal({t.precision},{t.scale})"
    if pt.is_date(t):
        return "date"
    if pt.is_timestamp(t):
        return "timestamp" if t.tz else "timestamp_ntz"
    return str(t)


def table_path(root: str, name: str) -> Path:
    return Path(root.removeprefix("file://")) / name


def exists(root: str, name: str) -> bool:
    p = table_path(root, name)
    return p.is_dir() and ((p / "_delta_log").is_dir() or any(p.rglob("*.parquet")))


def load_table(root: str, name: str) -> Loaded:
    """Load ``<root>/<name>``; raise FileNotFoundError when absent."""
    import pyarrow as pa
    import pyarrow.compute as pc

    p = table_path(root, name)
    if not exists(root, name):
        raise FileNotFoundError(f"no table at {p}")
    if (p / "_delta_log").is_dir():
        from deltalake import DeltaTable

        tbl = DeltaTable(str(p)).to_pyarrow_table()
        kind = "delta"
    else:
        import pyarrow.dataset as ds

        tbl = ds.dataset(str(p), format="parquet", partitioning="hive").to_table()
        kind = "parquet"
    types = {f.name: simple_type(f.type) for f in tbl.schema}
    # Small ints widen to int64 (types were captured above) so derived arithmetic cannot overflow;
    # decimals become float64 (cent-exact for DECIMAL(12,2)); tz-aware timestamps become naive UTC.
    cols = []
    for f in tbl.schema:
        col = tbl.column(f.name)
        if pa.types.is_decimal(f.type):
            col = pc.cast(col, pa.float64())
        elif pa.types.is_integer(f.type) and f.type.bit_width < 64:
            col = pc.cast(col, pa.int64())  # no int16 overflow in derived arithmetic
        elif pa.types.is_timestamp(f.type) and f.type.tz:
            col = pc.cast(col, pa.timestamp(f.type.unit))
        cols.append(col)
    tbl = pa.table(cols, names=tbl.schema.names)
    frame = tbl.to_pandas(date_as_object=False)
    return Loaded(frame, types, kind)
