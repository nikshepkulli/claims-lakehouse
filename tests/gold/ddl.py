"""Parse warehouse/snowflake/01_gold_ddl.sql into Spark column specs (pure Python, no Spark)."""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DDL = ROOT / "warehouse" / "snowflake" / "01_gold_ddl.sql"


def to_spark(sf_type: str) -> str:
    """Snowflake DDL type -> Spark simpleString (the mapping documented in gold/schema.py)."""
    t = re.sub(r"\s+", "", sf_type.upper())
    if re.fullmatch(r"VARCHAR\(\d+\)", t):
        return "string"
    m = re.fullmatch(r"NUMBER\((\d+),(\d+)\)", t)
    if m:
        return f"decimal({m.group(1)},{m.group(2)})"
    mapping = {
        "SMALLINT": "smallint",
        "INTEGER": "int",
        "DATE": "date",
        "BOOLEAN": "boolean",
        "TIMESTAMP_NTZ": "timestamp_ntz",
    }
    if t not in mapping:
        raise AssertionError(f"DDL type {sf_type!r} has no Spark mapping; extend ddl.to_spark")
    return mapping[t]


def varchar_lengths(table: str) -> dict[str, int]:
    out = {}
    for name, raw, _ in _raw_columns()[table]:
        m = re.fullmatch(r"VARCHAR\((\d+)\)", re.sub(r"\s+", "", raw.upper()))
        if m:
            out[name] = int(m.group(1))
    return out


def _raw_columns() -> dict[str, list[tuple[str, str, bool]]]:
    sql = re.sub(r"--[^\n]*", "", DDL.read_text())
    tables: dict[str, list[tuple[str, str, bool]]] = {}
    for m in re.finditer(r"CREATE TABLE IF NOT EXISTS (\w+) \((.*?)\n\)\s*COMMENT", sql, re.S):
        cols = []
        for raw in re.split(r",\s*\n", m.group(2)):
            line = raw.strip()
            if not line or line.upper().startswith("CONSTRAINT"):
                continue
            cm = re.match(r"(\w+)\s+([A-Z_]+(?:\s*\(\s*\d+(?:\s*,\s*\d+)?\s*\))?)(.*)$", line, re.S)
            assert cm, f"cannot parse DDL column: {line!r}"
            cols.append((cm.group(1).lower(), cm.group(2), "NOT NULL" in cm.group(3).upper()))
        tables[m.group(1).lower()] = cols
    return tables


def ddl_contract() -> dict[str, list[tuple[str, str, bool]]]:
    """table -> [(column, spark type, nullable)] in DDL declaration order."""
    return {
        t: [(n, to_spark(raw), not nn) for n, raw, nn in cols] for t, cols in _raw_columns().items()
    }
