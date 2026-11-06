"""SQL guardrails: each rule has a rejecting and (where relevant) an accepting case."""

import pytest

from lakehouse.genai.config import GenAIConfig
from lakehouse.genai.guardrails import check_sql

OK = "SELECT claim_type, COUNT(*) AS n FROM fact_claim GROUP BY claim_type"


def rules(sql, catalog, **cfg):
    d = check_sql(sql, catalog, GenAIConfig(**cfg) if cfg else None)
    return d.allowed, d.rules


def test_valid_query_allowed_and_limit_injected(analyst_catalog):
    d = check_sql(OK, analyst_catalog)
    assert d.allowed and d.limit_applied == "injected"
    assert d.sql.rstrip().endswith("LIMIT 100") and d.tables == ["fact_claim"]


def test_existing_small_limit_kept(analyst_catalog):
    d = check_sql("SELECT claim_id FROM fact_claim LIMIT 7", analyst_catalog)
    assert d.allowed and d.limit_applied is None and d.sql.endswith("LIMIT 7")


@pytest.mark.parametrize("lim", ["100000", "(SELECT 5)"])
def test_oversized_or_dynamic_limit_clamped(analyst_catalog, lim):
    if lim.startswith("("):
        # a scalar subquery uses a function-free path; still must be clamped or rejected
        d = check_sql(f"SELECT claim_id FROM fact_claim LIMIT {lim}", analyst_catalog)
        assert (not d.allowed) or d.sql.endswith("LIMIT 100")
    else:
        d = check_sql(f"SELECT claim_id FROM fact_claim LIMIT {lim}", analyst_catalog)
        assert d.allowed and d.limit_applied == "clamped" and d.sql.endswith("LIMIT 100")


def test_union_gets_limit(analyst_catalog):
    d = check_sql(
        "SELECT claim_id FROM fact_claim UNION SELECT claim_id FROM fact_claim_line",
        analyst_catalog,
    )
    assert d.allowed and d.sql.endswith("LIMIT 100")


def test_star_expands_to_allowed_columns_only(analyst_catalog):
    d = check_sql("SELECT * FROM dim_member", analyst_catalog)
    assert d.allowed
    for hidden in ("member_id", "birth_year", "enrollment_date", "record_hash"):
        assert hidden not in d.sql


@pytest.mark.parametrize(
    "sql,rule",
    [
        ("SELECT member_id FROM dim_member", "pii_column"),
        ("SELECT m.birth_year FROM dim_member m", "pii_column"),
        ("SELECT enrollment_date FROM dim_member", "pii_column"),
        ("SELECT provider_id FROM dim_provider", "pii_column"),
        ("SELECT nonexistent FROM fact_claim", "unknown_column"),
        ("DROP TABLE fact_claim", "not_select"),
        ("UPDATE fact_claim SET claim_status = 'PAID'", "not_select"),
        ("DELETE FROM fact_claim", "not_select"),
        ("INSERT INTO fact_claim VALUES (1)", "not_select"),
        ("CREATE TABLE x AS SELECT 1", "not_select"),
        ("SELECT * INTO x FROM fact_claim", "not_select"),
        ("PRAGMA database_list", "not_select"),
        ("SELECT 1; SELECT 2", "multi_statement"),
        ("SELECT 1 FROM fact_claim; DROP TABLE fact_claim", "multi_statement"),
        ("SELECT claim_id FROM fact_claim -- hi", "comment"),
        ("SELECT claim_id /* x */ FROM fact_claim", "comment"),
        ("SELEC claim_id FROM", "parse_error"),
        ("", "empty"),
        ("SELECT * FROM patients", "table_not_allowed"),
        ("SELECT * FROM information_schema.tables", "table_not_allowed"),
        ("SELECT * FROM main.fact_claim", "table_not_allowed"),
        ("SELECT * FROM read_parquet('x.parquet')", "table_function"),
        ("SELECT * FROM range(100000000)", "table_function"),
        ("SELECT getenv('HOME')", "function_not_allowed"),
        ("SELECT random() FROM fact_claim", "function_not_allowed"),
        ("SELECT current_setting('threads')", "function_not_allowed"),
        ("SELECT a.claim_id FROM fact_claim a CROSS JOIN fact_claim b", "cross_join"),
        ("SELECT a.claim_id FROM fact_claim a, fact_claim b", "cross_join"),
        ("SELECT a.claim_id FROM fact_claim a JOIN fact_claim b ON 1 = 1", "cross_join"),
        (
            "SELECT a.claim_id FROM fact_claim a JOIN fact_claim b ON a.claim_id > b.claim_id",
            "cross_join",
        ),
        (
            "WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM t) SELECT * FROM t",
            "recursive_cte",
        ),
    ],
)
def test_rejections_have_rule_and_reason(analyst_catalog, sql, rule):
    d = check_sql(sql, analyst_catalog)
    assert not d.allowed and d.sql is None
    assert rule in d.rules, d.reasons
    assert all(v.reason for v in d.violations)


def test_pii_reason_names_classification(analyst_catalog):
    d = check_sql("SELECT member_id FROM dim_member", analyst_catalog)
    assert "PHI_LIKE direct identifier" in d.reasons[0]


def test_join_cap_and_cost_cap(analyst_catalog):
    joins = " ".join(f"JOIN dim_date d{i} ON f.service_date_key = d{i}.date_key" for i in range(6))
    d = check_sql(f"SELECT COUNT(*) FROM fact_claim f {joins}", analyst_catalog)
    assert "join_cap" in d.rules
    ok, r = rules(
        "SELECT COUNT(*) FROM fact_claim f JOIN fact_claim_line l ON f.claim_id = l.claim_id",
        analyst_catalog,
        max_cost_units=10,
    )
    assert not ok and "cost_cap" in r


def test_sql_length_cap(analyst_catalog):
    d = check_sql(
        "SELECT claim_id FROM fact_claim WHERE claim_type = '" + "x" * 5000 + "'", analyst_catalog
    )
    assert d.rules == ["too_long"]


def test_public_role_cannot_use_analyst_columns(analyst_catalog):
    from lakehouse.genai.catalog import build_catalog

    pub = build_catalog("public")
    assert check_sql("SELECT total_paid_amount FROM fact_claim", analyst_catalog).allowed
    d = check_sql("SELECT total_paid_amount FROM fact_claim", pub)
    assert not d.allowed
    d = check_sql("SELECT member_sk FROM dim_member", pub)
    assert not d.allowed


def test_window_and_case_and_cte_allowed(analyst_catalog):
    sql = (
        "WITH t AS (SELECT claim_type, COUNT(*) c FROM fact_claim GROUP BY 1) "
        "SELECT claim_type, RANK() OVER (ORDER BY c DESC), "
        "CASE WHEN c > 1 THEN 'a' ELSE 'b' END FROM t"
    )
    assert check_sql(sql, analyst_catalog).allowed
