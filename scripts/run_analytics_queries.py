"""Execute every analyst query in analytics/queries/ against the built DuckDB marts (ticket #17).

Each .sql file starts with a comment header (see analytics/README in docs/analytics-guide.md):

    -- question: ...        what the analyst is asking
    -- grain: ...           what one result row is
    -- caveat: ...          what could mislead the reader
    -- columns: a, b, c     EXACT output column names, in order (checked)
    -- key_columns: a, b    columns that must never be NULL (checked)
    -- rows: ...            expected size, free text (documentation only)

A line starting with `--` and two or more spaces continues the previous field.
For every query this script checks: header complete, only `marts.mart_*` tables are
referenced, the query runs, it returns >= 1 row, the output columns equal `columns`, and
no key column contains NULL. Exit code 1 on any failure. All data is synthetic.

Usage (after `dbt build --target duckdb`):
    python scripts/run_analytics_queries.py [--db data/claims.duckdb] [--queries analytics/queries]
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REQUIRED_FIELDS = ("question", "grain", "caveat", "columns", "key_columns", "rows")
LIST_FIELDS = ("columns", "key_columns")
_FIELD = re.compile(r"^--\s*([a-z_]+):\s*(.*)$")
_CONTINUATION = re.compile(r"^--\s{2,}(\S.*)$")
# Analyst queries must read the marts only: never staging, intermediate, raw or lake layers.
_FORBIDDEN_LAYERS = re.compile(r"\b(stg_\w+|int_\w+|raw|bronze|silver|gold|staging)\b", re.I)
_TABLE_REF = re.compile(r"\b(?:FROM|JOIN)\s+([A-Za-z_][\w.]*)", re.I)


class QueryError(Exception):
    """A query file is malformed or violated its declared contract."""


def parse_header(text: str) -> dict[str, str | list[str]]:
    """Parse the leading `-- field: value` comment block. List fields become lists.

    Stops at the first line that is not a comment. Raises QueryError on a missing field.
    """
    fields: dict[str, str] = {}
    last: str | None = None
    for line in text.splitlines():
        line = line.rstrip()
        if not line.startswith("--"):
            break
        match = _FIELD.match(line)
        if match and match.group(1) in REQUIRED_FIELDS:
            last = match.group(1)
            fields[last] = match.group(2).strip()
        elif last and (cont := _CONTINUATION.match(line)):
            fields[last] += " " + cont.group(1).strip()
    missing = [f for f in REQUIRED_FIELDS if not fields.get(f)]
    if missing:
        raise QueryError(f"header missing field(s): {', '.join(missing)}")
    parsed: dict[str, str | list[str]] = dict(fields)
    for name in LIST_FIELDS:
        parsed[name] = [c.strip() for c in fields[name].split(",") if c.strip()]
    unknown = set(parsed["key_columns"]) - set(parsed["columns"])
    if unknown:
        raise QueryError(f"key_columns not in columns: {sorted(unknown)}")
    return parsed


def sql_body(text: str) -> str:
    """The query with comment lines removed and any trailing semicolon dropped."""
    lines = [ln for ln in text.splitlines() if not ln.lstrip().startswith("--")]
    return "\n".join(lines).strip().rstrip(";").strip()


def check_tables(body: str) -> None:
    """Raise QueryError unless every FROM/JOIN target is a `marts.mart_*` table or a CTE."""
    ctes = {m.lower() for m in re.findall(r"\b(\w+)\s+AS\s*\(", body, re.I)}
    for ref in _TABLE_REF.findall(body):
        if ref.lower() in ctes:
            continue
        if not re.fullmatch(r"marts\.mart_\w+", ref, re.I) or _FORBIDDEN_LAYERS.search(ref):
            raise QueryError(f"reads {ref!r}: queries may only read marts.mart_* tables")


def run_query(con, path: Path) -> int:
    """Run one query file and check it against its header. Returns the row count."""
    text = path.read_text()
    header = parse_header(text)
    body = sql_body(text)
    check_tables(body)
    cur = con.execute(body)
    columns = [d[0] for d in cur.description]
    rows = cur.fetchall()
    if columns != header["columns"]:
        raise QueryError(f"columns {columns} != header {header['columns']}")
    if not rows:
        raise QueryError("returned 0 rows")
    for name in header["key_columns"]:
        idx = columns.index(name)
        nulls = sum(1 for r in rows if r[idx] is None)
        if nulls:
            raise QueryError(f"key column {name!r} has {nulls} NULL(s)")
    return len(rows)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument(
        "--db", default=os.environ.get("DBT_DUCKDB_PATH", str(ROOT / "data" / "claims.duckdb"))
    )
    parser.add_argument("--queries", default=str(ROOT / "analytics" / "queries"))
    args = parser.parse_args(argv)

    import duckdb  # imported here so the header parser works without duckdb installed

    files = sorted(Path(args.queries).glob("*.sql"))
    if not files:
        print(f"no .sql files in {args.queries}", file=sys.stderr)
        return 1
    con = duckdb.connect(args.db, read_only=True)
    failures = 0
    for path in files:
        try:
            n = run_query(con, path)
            print(f"PASS {path.name}: {n} row(s)")
        except (QueryError, duckdb.Error) as exc:
            failures += 1
            print(f"FAIL {path.name}: {exc}")
    print(f"{len(files) - failures}/{len(files)} queries passed")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
