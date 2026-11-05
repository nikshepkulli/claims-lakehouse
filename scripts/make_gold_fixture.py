"""Build a gold-shaped Parquet TEST FIXTURE from claimsgen output (ticket #15).

*** TEST FIXTURE (fallback). This is NOT the lakehouse gold layer. ***
The real gold layer (Delta, SCD2 dimensions, reconciliation) is built by Spark in
src/lakehouse/gold (ticket #8, docs/gold.md) and is the primary dbt source: the
`pipeline e2e` CI job builds dbt on its Parquet export. This script remains a fast,
Spark-free approximation so the dbt project (dbt/) can be built and tested against the
gold CONTRACT (the Snowflake DDL in warehouse/snowflake/01_gold_ddl.sql) without Java, and
it injects a second SCD2 version that generated gold does not have. All data is synthetic.

What it does, in DuckDB SQL, to approximate silver -> gold:
  * dedupe claims, lines and diagnoses (the generator re-sends some claims);
  * quarantine whole claims that have a null/invalid procedure code, a negative line
    amount or an orphan member (keeps header = sum(lines) true, as gold would);
  * null the primary diagnosis when its ICD-10 code is invalid (dim_diagnosis holds only
    valid codes, per the DDL comment);
  * build dim_member / dim_provider as SCD2 with a deterministic subset of entities
    getting a second version on 2024-07-01, and point facts at the version valid on the
    service date, so the SCD2 tests in dbt have something real to check;
  * cast every column to the exact DDL type (GOLD_SCHEMA below).

Using the real gold instead: GOLD_PATH=<gold root>/export (or dbt --vars
'{gold_path: ...}'), see docs/dbt.md. tests/dbt/test_gold_contract.py keeps GOLD_SCHEMA, the
DDL and the dbt source YAML in lockstep, so any divergence fails CI.

Usage:
    PYTHONPATH=src python -m claimsgen --rows 5000 --out data/raw
    python scripts/make_gold_fixture.py --raw data/raw --out data/gold
"""

from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

# (column, duckdb type, not_null). Mirrors warehouse/snowflake/01_gold_ddl.sql; the
# contract test fails if this drifts from the DDL.
GOLD_SCHEMA: dict[str, list[tuple[str, str, bool]]] = {
    "dim_date": [
        ("date_key", "INTEGER", True),
        ("full_date", "DATE", True),
        ("year", "SMALLINT", True),
        ("quarter", "SMALLINT", True),
        ("month", "SMALLINT", True),
        ("month_name", "VARCHAR", True),
        ("day_of_month", "SMALLINT", True),
        ("day_of_week", "SMALLINT", True),
        ("is_weekend", "BOOLEAN", True),
    ],
    "dim_member": [
        ("member_sk", "VARCHAR", True),
        ("member_id", "VARCHAR", True),
        ("birth_year", "SMALLINT", False),
        ("sex", "VARCHAR", False),
        ("state", "VARCHAR", False),
        ("plan_type", "VARCHAR", False),
        ("enrollment_date", "DATE", False),
        ("effective_from", "TIMESTAMP", True),
        ("effective_to", "TIMESTAMP", False),
        ("is_current", "BOOLEAN", True),
        ("record_hash", "VARCHAR", False),
        ("load_batch_id", "VARCHAR", True),
        ("loaded_at", "TIMESTAMP", True),
    ],
    "dim_provider": [
        ("provider_sk", "VARCHAR", True),
        ("provider_id", "VARCHAR", True),
        ("specialty", "VARCHAR", False),
        ("state", "VARCHAR", False),
        ("network_status", "VARCHAR", False),
        ("cost_tier", "VARCHAR", False),
        ("effective_from", "TIMESTAMP", True),
        ("effective_to", "TIMESTAMP", False),
        ("is_current", "BOOLEAN", True),
        ("record_hash", "VARCHAR", False),
        ("load_batch_id", "VARCHAR", True),
        ("loaded_at", "TIMESTAMP", True),
    ],
    "dim_diagnosis": [
        ("diagnosis_sk", "VARCHAR", True),
        ("icd10_code", "VARCHAR", True),
        ("icd10_chapter", "VARCHAR", False),
        ("description", "VARCHAR", False),
        ("load_batch_id", "VARCHAR", True),
        ("loaded_at", "TIMESTAMP", True),
    ],
    "fact_claim": [
        ("claim_id", "VARCHAR", True),
        ("member_sk", "VARCHAR", True),
        ("provider_sk", "VARCHAR", True),
        ("primary_diagnosis_sk", "VARCHAR", False),
        ("service_date_key", "INTEGER", True),
        ("received_date_key", "INTEGER", True),
        ("claim_type", "VARCHAR", True),
        ("place_of_service", "VARCHAR", False),
        ("claim_status", "VARCHAR", True),
        ("line_count", "SMALLINT", True),
        ("total_billed_amount", "DECIMAL(12,2)", True),
        ("total_allowed_amount", "DECIMAL(12,2)", True),
        ("total_paid_amount", "DECIMAL(12,2)", True),
        ("load_batch_id", "VARCHAR", True),
        ("loaded_at", "TIMESTAMP", True),
    ],
    "fact_claim_line": [
        ("line_id", "VARCHAR", True),
        ("claim_id", "VARCHAR", True),
        ("line_number", "SMALLINT", True),
        ("procedure_code", "VARCHAR", True),
        ("units", "SMALLINT", True),
        ("service_date_key", "INTEGER", True),
        ("billed_amount", "DECIMAL(12,2)", True),
        ("allowed_amount", "DECIMAL(12,2)", True),
        ("paid_amount", "DECIMAL(12,2)", True),
        ("load_batch_id", "VARCHAR", True),
        ("loaded_at", "TIMESTAMP", True),
    ],
}

BATCH_ID = "FIXTURE-NOT-SPARK-GOLD"
SCD2_SPLIT = "2024-07-01"

ICD_RE = r"^[A-TV-Z]\d[0-9A-Z](\.[0-9A-Z]{1,4})?$"
PROC_RE = r"^(\d{5}|[A-V]\d{4})$"


def _glob(raw: Path, table: str) -> str:
    return f"{(raw / table).as_posix()}/*/*.parquet"


def _select(table: str, source: str, exprs: dict[str, str]) -> str:
    """SELECT casting each column to its DDL type, in DDL column order."""
    cols = []
    for name, typ, _ in GOLD_SCHEMA[table]:
        cols.append(f"CAST({exprs.get(name, name)} AS {typ}) AS {name}")
    return "SELECT " + ", ".join(cols) + f" FROM {source}"


def build(raw: Path, out: Path) -> dict[str, int]:
    import duckdb

    sys.path.insert(0, str(ROOT / "src"))
    from claimsgen.reference import DIAGNOSES

    con = duckdb.connect()
    for t in ("members", "providers", "claims", "claim_lines", "diagnoses"):
        con.execute(
            f"CREATE VIEW raw_{t} AS "
            f"SELECT * FROM read_parquet('{_glob(raw, t)}', union_by_name = true)"
        )
    con.execute("CREATE TABLE icd_desc (code VARCHAR, description VARCHAR)")
    con.executemany("INSERT INTO icd_desc VALUES (?, ?)", [(c, d) for c, d, _ in DIAGNOSES])

    # dimensions: current (type 1) member/provider rows, then SCD2 versions
    con.execute(
        """CREATE TABLE members_1 AS
           SELECT DISTINCT ON (member_id) * FROM raw_members
           ORDER BY member_id, snapshot_date DESC"""
    )
    con.execute(
        """CREATE TABLE providers_1 AS
           SELECT DISTINCT ON (provider_id) * FROM raw_providers
           ORDER BY provider_id, snapshot_date DESC"""
    )
    con.execute(
        "CREATE TABLE plan_types AS SELECT list(DISTINCT plan_type ORDER BY 1) AS p FROM members_1"
    )
    con.execute("CREATE TABLE cost_tiers AS SELECT ['STANDARD', 'ELEVATED', 'HIGH_COST'] AS p")

    # Every 20th member / 25th provider (by numeric id suffix) gets a second version.
    # Version 1 carries the original attribute; version 2 (from SCD2_SPLIT) rotates it.
    con.execute(
        f"""CREATE TABLE member_versions AS
        WITH base AS (
            SELECT m.*, CAST(right(member_id, 4) AS INTEGER) % 20 = 0 AS changes
            FROM members_1 m
        ), v AS (
            SELECT *, 1 AS ver FROM base
            UNION ALL
            SELECT * , 2 AS ver FROM base WHERE changes
        )
        SELECT
            v.member_id, birth_year, sex, state,
            CASE WHEN ver = 2
                THEN (SELECT p[(list_position(p, v.plan_type) % len(p)) + 1] FROM plan_types)
                ELSE v.plan_type END AS plan_type,
            enrollment_date,
            CASE WHEN ver = 1 THEN TIMESTAMP '1900-01-01 00:00:00'
                 ELSE TIMESTAMP '{SCD2_SPLIT} 00:00:00' END AS effective_from,
            CASE WHEN ver = 1 AND changes
                 THEN TIMESTAMP '{SCD2_SPLIT} 00:00:00' END AS effective_to,
            NOT (ver = 1 AND changes) AS is_current
        FROM v"""
    )
    con.execute(
        f"""CREATE TABLE provider_versions AS
        WITH base AS (
            SELECT p.*, CAST(right(provider_id, 4) AS INTEGER) % 25 = 0 AS changes
            FROM providers_1 p
        ), v AS (
            SELECT *, 1 AS ver FROM base
            UNION ALL
            SELECT * , 2 AS ver FROM base WHERE changes
        )
        SELECT
            v.provider_id, specialty, state, network_status,
            CASE WHEN ver = 2
                THEN (SELECT p[(list_position(p, v.cost_tier) % len(p)) + 1] FROM cost_tiers)
                ELSE v.cost_tier END AS cost_tier,
            CASE WHEN ver = 1 THEN TIMESTAMP '1900-01-01 00:00:00'
                 ELSE TIMESTAMP '{SCD2_SPLIT} 00:00:00' END AS effective_from,
            CASE WHEN ver = 1 AND changes
                 THEN TIMESTAMP '{SCD2_SPLIT} 00:00:00' END AS effective_to,
            NOT (ver = 1 AND changes) AS is_current
        FROM v"""
    )

    # silver-ish cleaning of the facts
    con.execute(
        "CREATE TABLE claims_d AS SELECT DISTINCT ON (claim_id) * FROM raw_claims "
        "ORDER BY claim_id, ingest_date"
    )
    con.execute(
        "CREATE TABLE lines_d AS SELECT DISTINCT ON (line_id) * FROM raw_claim_lines "
        "ORDER BY line_id, ingest_date"
    )
    con.execute(
        "CREATE TABLE dx_d AS SELECT DISTINCT ON (claim_id, dx_seq) * FROM raw_diagnoses "
        "ORDER BY claim_id, dx_seq, ingest_date"
    )
    con.execute(
        f"""CREATE TABLE bad_claims AS
        SELECT claim_id FROM lines_d
        WHERE procedure_code IS NULL OR NOT regexp_matches(procedure_code, '{PROC_RE}')
           OR billed_amount < 0 OR allowed_amount < 0 OR paid_amount < 0
        UNION
        SELECT claim_id FROM claims_d WHERE member_id NOT IN (SELECT member_id FROM members_1)
        UNION
        SELECT claim_id FROM claims_d
        WHERE provider_id NOT IN (SELECT provider_id FROM providers_1)"""
    )
    con.execute(
        "CREATE TABLE claims_ok AS SELECT * FROM claims_d "
        "WHERE claim_id NOT IN (SELECT claim_id FROM bad_claims) "
        "AND claim_id IN (SELECT claim_id FROM lines_d)"
    )
    con.execute(
        f"""CREATE TABLE dx_ok AS
        SELECT * FROM dx_d
        WHERE claim_id IN (SELECT claim_id FROM claims_ok) AND icd10_code IS NOT NULL
          AND regexp_matches(icd10_code, '{ICD_RE}')"""
    )

    sk = "md5({id} || '|' || CAST(effective_from AS VARCHAR))"
    views: dict[str, str] = {}
    views["dim_date"] = _select(
        "dim_date",
        "(SELECT CAST(d AS DATE) AS full_date FROM generate_series("
        "DATE '2023-10-01', DATE '2025-12-31', INTERVAL 1 DAY) t(d))",
        {
            "date_key": "CAST(strftime(full_date, '%Y%m%d') AS INTEGER)",
            "year": "year(full_date)",
            "quarter": "quarter(full_date)",
            "month": "month(full_date)",
            "month_name": "monthname(full_date)",
            "day_of_month": "day(full_date)",
            "day_of_week": "isodow(full_date)",
            "is_weekend": "isodow(full_date) >= 6",
        },
    )
    views["dim_member"] = _select(
        "dim_member",
        "member_versions",
        {
            "member_sk": sk.format(id="member_id"),
            "record_hash": "md5(concat_ws('|', birth_year, sex, state, plan_type, "
            "enrollment_date))",
            "load_batch_id": f"'{BATCH_ID}'",
            "loaded_at": "now()",
        },
    )
    views["dim_provider"] = _select(
        "dim_provider",
        "provider_versions",
        {
            "provider_sk": sk.format(id="provider_id"),
            "record_hash": "md5(concat_ws('|', specialty, state, network_status, cost_tier))",
            "load_batch_id": f"'{BATCH_ID}'",
            "loaded_at": "now()",
        },
    )
    views["dim_diagnosis"] = _select(
        "dim_diagnosis",
        "(SELECT c.icd10_code, d.description FROM (SELECT DISTINCT icd10_code FROM dx_ok) c "
        "LEFT JOIN icd_desc d ON d.code = c.icd10_code)",
        {
            "diagnosis_sk": "md5(icd10_code)",
            "icd10_chapter": "left(icd10_code, 1)",
            "description": "coalesce(description, 'Synthetic ICD-10 code')",
            "load_batch_id": f"'{BATCH_ID}'",
            "loaded_at": "now()",
        },
    )
    views["fact_claim"] = _select(
        "fact_claim",
        """(SELECT c.*,
                   mv.member_id || '|' || CAST(mv.effective_from AS VARCHAR) AS m_key,
                   pv.provider_id || '|' || CAST(pv.effective_from AS VARCHAR) AS p_key,
                   (SELECT icd10_code FROM dx_ok x
                    WHERE x.claim_id = c.claim_id AND x.diagnosis_type = 'PRIMARY'
                    ORDER BY dx_seq LIMIT 1) AS primary_code
            FROM claims_ok c
            JOIN member_versions mv ON mv.member_id = c.member_id
             AND c.service_date >= CAST(mv.effective_from AS DATE)
             AND (mv.effective_to IS NULL OR c.service_date < CAST(mv.effective_to AS DATE))
            JOIN provider_versions pv ON pv.provider_id = c.provider_id
             AND c.service_date >= CAST(pv.effective_from AS DATE)
             AND (pv.effective_to IS NULL OR c.service_date < CAST(pv.effective_to AS DATE))
           )""",
        {
            "member_sk": "md5(m_key)",
            "provider_sk": "md5(p_key)",
            "primary_diagnosis_sk": "CASE WHEN primary_code IS NULL THEN NULL "
            "ELSE md5(primary_code) END",
            "service_date_key": "CAST(strftime(service_date, '%Y%m%d') AS INTEGER)",
            "received_date_key": "CAST(strftime(received_date, '%Y%m%d') AS INTEGER)",
            "load_batch_id": f"'{BATCH_ID}'",
            "loaded_at": "now()",
        },
    )
    views["fact_claim_line"] = _select(
        "fact_claim_line",
        "(SELECT l.* FROM lines_d l WHERE l.claim_id IN (SELECT claim_id FROM claims_ok))",
        {
            "service_date_key": "CAST(strftime(service_date, '%Y%m%d') AS INTEGER)",
            "load_batch_id": f"'{BATCH_ID}'",
            "loaded_at": "now()",
        },
    )

    counts: dict[str, int] = {}
    for table, query in views.items():
        target = out / table
        if target.exists():
            shutil.rmtree(target)
        target.mkdir(parents=True)
        con.execute(
            f"COPY ({query}) TO '{(target / 'part-00000.parquet').as_posix()}' (FORMAT parquet)"
        )
        counts[table] = con.execute(
            f"SELECT count(*) FROM read_parquet('{(target / '*.parquet').as_posix()}')"
        ).fetchone()[0]
    return counts


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--raw", type=Path, default=Path("data/raw"), help="claimsgen output root")
    ap.add_argument("--out", type=Path, default=Path("data/gold"), help="gold fixture root")
    args = ap.parse_args(argv)
    if not (args.raw / "claims").exists():
        ap.error(
            f"{args.raw}/claims not found; run: PYTHONPATH=src python -m claimsgen --rows 5000"
        )
    counts = build(args.raw, args.out)
    print("TEST FIXTURE (not Spark gold, ticket #8). Rows written:")
    for t, n in counts.items():
        print(f"  {t}: {n:,}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
