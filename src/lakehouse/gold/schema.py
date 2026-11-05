"""The gold star schema, column by column. This is the in-code copy of the Snowflake DDL.

``warehouse/snowflake/01_gold_ddl.sql`` is the CONTRACT (the dbt sources and the Snowflake load
are written against it). ``tests/gold/test_gold_contract.py`` parses the DDL and fails if the
names, order, types or NOT NULL flags here drift from it, and the Spark tests check that the
Delta tables the job really writes carry exactly this schema.

Types are Spark ``simpleString`` names. The DDL -> Spark mapping the contract test applies:
``VARCHAR(n)`` -> ``string``, ``SMALLINT`` -> ``smallint``, ``INTEGER`` -> ``int``,
``NUMBER(p,s)`` -> ``decimal(p,s)``, ``DATE`` -> ``date``, ``BOOLEAN`` -> ``boolean``,
``TIMESTAMP_NTZ`` -> ``timestamp_ntz`` (a wall-clock timestamp with no zone, which is also what
a Parquet reader such as DuckDB sees in the export).
"""

from __future__ import annotations

from datetime import datetime
from typing import NamedTuple


class Col(NamedTuple):
    name: str
    type: str  # Spark simpleString
    nullable: bool


def _c(name: str, type_: str, nullable: bool = False) -> Col:
    return Col(name, type_, nullable)


# Delta's default retention window. VACUUM below it can delete files a concurrent reader needs.
MIN_VACUUM_HOURS = 168

AMOUNT = "decimal(12,2)"

# Start of time for the first version of a dimension member. The generator sends one snapshot, so
# the first version must be valid for claims dated before that snapshot: effective_from is this
# constant and every claim finds a version (docs/gold.md, "SCD2 rules").
EPOCH = datetime(1900, 1, 1)

# Star-schema tables in load order (dimensions before facts).
TABLES: dict[str, list[Col]] = {
    "dim_date": [
        _c("date_key", "int"),
        _c("full_date", "date"),
        _c("year", "smallint"),
        _c("quarter", "smallint"),
        _c("month", "smallint"),
        _c("month_name", "string"),
        _c("day_of_month", "smallint"),
        _c("day_of_week", "smallint"),
        _c("is_weekend", "boolean"),
    ],
    "dim_member": [
        _c("member_sk", "string"),
        _c("member_id", "string"),
        _c("birth_year", "smallint", True),
        _c("sex", "string", True),
        _c("state", "string", True),
        _c("plan_type", "string", True),
        _c("enrollment_date", "date", True),
        _c("effective_from", "timestamp_ntz"),
        _c("effective_to", "timestamp_ntz", True),
        _c("is_current", "boolean"),
        _c("record_hash", "string", True),
        _c("load_batch_id", "string"),
        _c("loaded_at", "timestamp_ntz"),
    ],
    "dim_provider": [
        _c("provider_sk", "string"),
        _c("provider_id", "string"),
        _c("specialty", "string", True),
        _c("state", "string", True),
        _c("network_status", "string", True),
        _c("cost_tier", "string", True),
        _c("effective_from", "timestamp_ntz"),
        _c("effective_to", "timestamp_ntz", True),
        _c("is_current", "boolean"),
        _c("record_hash", "string", True),
        _c("load_batch_id", "string"),
        _c("loaded_at", "timestamp_ntz"),
    ],
    "dim_diagnosis": [
        _c("diagnosis_sk", "string"),
        _c("icd10_code", "string"),
        _c("icd10_chapter", "string", True),
        _c("description", "string", True),
        _c("load_batch_id", "string"),
        _c("loaded_at", "timestamp_ntz"),
    ],
    "fact_claim": [
        _c("claim_id", "string"),
        _c("member_sk", "string"),
        _c("provider_sk", "string"),
        _c("primary_diagnosis_sk", "string", True),
        _c("service_date_key", "int"),
        _c("received_date_key", "int"),
        _c("claim_type", "string"),
        _c("place_of_service", "string", True),
        _c("claim_status", "string"),
        _c("line_count", "smallint"),
        _c("total_billed_amount", AMOUNT),
        _c("total_allowed_amount", AMOUNT),
        _c("total_paid_amount", AMOUNT),
        _c("load_batch_id", "string"),
        _c("loaded_at", "timestamp_ntz"),
    ],
    "fact_claim_line": [
        _c("line_id", "string"),
        _c("claim_id", "string"),
        _c("line_number", "smallint"),
        _c("procedure_code", "string"),
        _c("units", "smallint"),
        _c("service_date_key", "int"),
        _c("billed_amount", AMOUNT),
        _c("allowed_amount", AMOUNT),
        _c("paid_amount", AMOUNT),
        _c("load_batch_id", "string"),
        _c("loaded_at", "timestamp_ntz"),
    ],
}

# Delta partition column per table. Facts partition by the batch that loaded them; dimensions are
# small and unpartitioned.
PARTITION_BY: dict[str, str] = {"fact_claim": "load_batch_id", "fact_claim_line": "load_batch_id"}

# Operational tables beside the star schema (not part of the Snowflake contract).
RECONCILIATION_COLS = [
    _c("gold_table", "string"),
    _c("batch_id", "string"),
    _c("batch_date", "date"),
    _c("silver_rows", "bigint"),
    _c("gold_rows", "bigint"),
    _c("excluded_rows", "bigint"),
    _c("silver_billed", "decimal(22,2)", True),
    _c("gold_billed", "decimal(22,2)", True),
    _c("excluded_billed", "decimal(22,2)", True),
    _c("silver_allowed", "decimal(22,2)", True),
    _c("gold_allowed", "decimal(22,2)", True),
    _c("excluded_allowed", "decimal(22,2)", True),
    _c("silver_paid", "decimal(22,2)", True),
    _c("gold_paid", "decimal(22,2)", True),
    _c("excluded_paid", "decimal(22,2)", True),
    _c("key_violations", "bigint"),
    _c("identity_ok", "boolean"),
    _c("detail", "string", True),
    _c("_run_ts", "timestamp"),
]

EXCLUSION_COLS = [
    _c("dataset", "string"),
    _c("business_key", "string"),
    _c("reason_code", "string"),
    _c("reason_detail", "string", True),
    _c("batch_id", "string"),
    _c("_run_ts", "timestamp"),
]


def column_names(table: str) -> list[str]:
    return [c.name for c in TABLES[table]]
