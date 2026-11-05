"""Generate the Snowflake tag-assignment scripts from the classification registry.

    python -m lakehouse.governance.snowflake_sql          # (re)write the two scripts
    python -m lakehouse.governance.snowflake_sql --check  # exit 1 if they are stale

Outputs (committed, so reviewers can read them; a test fails if they drift):
    warehouse/snowflake/91_apply_tags_gold.sql    gold tables from 01_gold_ddl.sql
    warehouse/snowflake/92_apply_tags_marts.sql   dbt marts, run AFTER every dbt build
"""

from __future__ import annotations

import sys
from pathlib import Path

from .registry import REPO_ROOT, Table, load

OUT_DIR = REPO_ROOT / "warehouse" / "snowflake"
OUTPUTS = {"gold": "91_apply_tags_gold.sql", "mart": "92_apply_tags_marts.sql"}

_HEADERS = {
    "gold": (
        "-- GENERATED from governance/data_classification.yaml by\n"
        "--   python -m lakehouse.governance.snowflake_sql\n"
        "-- Do not edit by hand; a test fails if this file drifts from the registry.\n"
        "--\n"
        "-- Tags every column of the gold tables with its classification and, where the\n"
        "-- registry says so, the privacy handling that the tag-based masking policies\n"
        "-- (90_masking_policies.sql) enforce. Run as SYSADMIN (owns the tables and the tags)\n"
        "-- after 01_gold_ddl.sql and 90_masking_policies.sql, database selected by the\n"
        "-- connection. Idempotent: SET TAG overwrites. NOT EXECUTED against Snowflake.\n"
    ),
    "mart": (
        "-- GENERATED from governance/data_classification.yaml by\n"
        "--   python -m lakehouse.governance.snowflake_sql\n"
        "-- Do not edit by hand; a test fails if this file drifts from the registry.\n"
        "--\n"
        "-- Same as 91_apply_tags_gold.sql but for the dbt marts. dbt rebuilds marts with\n"
        "-- CREATE OR REPLACE TABLE, which drops column tags (and therefore the masking), so\n"
        "-- run this script after EVERY dbt build, as SYSADMIN (it inherits the TRANSFORMER\n"
        "-- role that owns the marts). Until it has run, the marts are UNPROTECTED.\n"
        "-- NOT EXECUTED against Snowflake.\n"
    ),
}


def render(tables: dict[str, Table], layer: str) -> str:
    out = [_HEADERS[layer], "USE SCHEMA marts;\n"]
    for table in tables.values():
        if table.layer != layer:
            continue
        out.append(f"\n-- {table.name}\n")
        for col in table.columns.values():
            tags = [("data_classification", col.classification)]
            if col.handling != "none":
                tags.append(("privacy_handling", col.handling.upper()))
            for tag, value in tags:
                out.append(
                    f"ALTER TABLE {table.name} MODIFY COLUMN {col.name} "
                    f"SET TAG secure.{tag} = '{value}';\n"
                )
    return "".join(out)


def main(argv: list[str] | None = None) -> int:
    check = "--check" in (argv if argv is not None else sys.argv[1:])
    tables = load()
    stale = []
    for layer, name in OUTPUTS.items():
        path: Path = OUT_DIR / name
        text = render(tables, layer)
        if check:
            if not path.exists() or path.read_text() != text:
                stale.append(name)
        else:
            path.write_text(text)
    if stale:
        print(f"stale (re-run python -m lakehouse.governance.snowflake_sql): {stale}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
