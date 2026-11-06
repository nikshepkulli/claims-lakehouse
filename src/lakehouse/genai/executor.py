"""Execute validated SQL on DuckDB over the gold Parquet export (and optionally dbt marts).

The sandbox is built per role: only the exposed columns are copied into in-memory tables, then
external access is disabled and the configuration locked. So even a query that slipped past the
guardrails cannot read a PII column, a file, or another database.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass
from pathlib import Path

import duckdb

from .catalog import Catalog


class QueryTimeout(RuntimeError):
    pass


@dataclass
class ExecResult:
    columns: list[str]
    rows: list[tuple]
    truncated: bool


def _q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


class Executor:
    def __init__(
        self,
        catalog: Catalog,
        gold_dir: Path,
        marts_db: Path | None = None,
        memory_limit: str = "512MB",
    ):
        self.catalog = catalog
        self.con = duckdb.connect()
        self.column_types: dict[str, dict[str, str]] = {}
        self.con.execute(f"SET memory_limit='{memory_limit}'")
        self.con.execute("SET threads=2")
        self.con.execute("SET max_temp_directory_size='256MB'")  # bound spill-to-disk
        marts = [t for t in catalog.tables.values() if t.layer == "mart"]
        if marts:
            if marts_db is None:
                raise ValueError("catalog includes mart tables; pass marts_db")
            self.con.execute(f"ATTACH '{marts_db}' AS src (READ_ONLY)")
        for t in catalog.tables.values():
            cols = ", ".join(_q(c) for c in t.allowed)
            if t.layer == "gold":
                files = sorted((Path(gold_dir) / t.name).glob("*.parquet"))
                if not files:
                    raise FileNotFoundError(f"no parquet files for {t.name} under {gold_dir}")
                src = f"read_parquet('{Path(gold_dir) / t.name}/*.parquet')"
            else:
                src = f"src.marts.{_q(t.name)}"
            self.con.execute(f"CREATE TABLE {_q(t.name)} AS SELECT {cols} FROM {src}")
            self.column_types[t.name] = {
                r[0]: r[1] for r in self.con.execute(f"DESCRIBE {_q(t.name)}").fetchall()
            }
        if marts:
            self.con.execute("DETACH src")
        self.con.execute("SET enable_external_access=false")
        self.con.execute("SET lock_configuration=true")

    def run(self, sql: str, timeout_s: float, max_rows: int) -> ExecResult:
        cur = self.con.cursor()
        timer = threading.Timer(timeout_s, cur.interrupt)
        timer.start()
        try:
            cur.execute(sql)
            rows = cur.fetchmany(max_rows + 1)
            cols = [d[0] for d in cur.description]
        except duckdb.InterruptException as exc:
            raise QueryTimeout(f"query exceeded {timeout_s}s and was cancelled") from exc
        finally:
            timer.cancel()
            cur.close()
        return ExecResult(cols, rows[:max_rows], len(rows) > max_rows)

    def close(self) -> None:
        self.con.close()
