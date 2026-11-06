"""The batch feature engine: DuckDB SQL over the prepared source tables.

Every feature is a pure function of ``(entity key, as_of)`` and the rows visible at ``as_of``.
The two conditions that enforce that are module constants (``HIST_COND``, ``OUTCOME_COND``) so
the leakage tests can break them on purpose and prove the guard (guards.py) notices.

Parity path (docs/features.md): the SQL uses ANSI-style windows/aggregates plus DuckDB's
``FILTER (WHERE ...)``, ``//`` integer division and ``regexp_matches``; Spark SQL and Snowflake
have direct equivalents (``COUNT_IF`` / ``CASE``, ``DIV0``/``FLOOR``, ``REGEXP_LIKE``).
"""

# ruff: noqa: E501  (embedded SQL)
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from .chronic import CONDITIONS
from .config import (
    ADJUDICATED,
    DENIED,
    EVENT_TIME_OF_DAY_SECONDS,
    WINDOWS,
    FeatureConfig,
)

if TYPE_CHECKING:
    import duckdb

# --- the two point-in-time conditions (c = a history claim, s = a spine row) -----------------
HIST_COND = "c.event_ts <= s.as_of"
OUTCOME_COND = "c.outcome_ts <= s.as_of AND c.status IN ({adj})".format(
    adj=", ".join(f"'{s}'" for s in ADJUDICATED)
)
# SCD2 as-of join (d = a dimension version, s = a spine row)
SCD2_COND = "d.effective_from <= s.as_of AND (d.effective_to IS NULL OR s.as_of < d.effective_to)"


def _w(days: int) -> str:
    return f"c.event_ts > s.as_of - INTERVAL {days} DAY"


@dataclass(frozen=True)
class Names:
    """Table names the engine reads, so the guard can swap in perturbed copies."""

    claims: str = "src_claims"  # own-claim attributes and the claim spine
    hist: str = "src_claims"  # history the aggregates are computed over
    dim_member: str = "src_dim_member"
    dim_provider: str = "src_dim_provider"


def prepare_sources(con: duckdb.DuckDBPyConnection, export_dir: str, cfg: FeatureConfig) -> None:
    """Materialise the three source tables from the gold Parquet export.

    ``src_claims`` joins fact_claim to the member/provider *natural* ids (the fact's surrogate
    keys point at the version valid on the service date; features re-resolve dimensions as of
    ``as_of`` by natural id) and to the primary ICD-10 code. Amounts become exact integer cents.
    """
    p = str(export_dir).rstrip("/")
    rd = lambda t: f"read_parquet('{p}/{t}/*.parquet')"  # noqa: E731
    d = lambda k: f"make_date({k} // 10000, ({k} // 100) % 100, {k} % 100)"  # noqa: E731
    con.execute(
        f"""
        CREATE OR REPLACE TABLE src_claims AS
        SELECT c.claim_id, m.member_id, pr.provider_id,
               {d("c.service_date_key")} AS service_date,
               {d("c.received_date_key")} AS received_date,
               CAST({d("c.received_date_key")} AS TIMESTAMP)
                   + INTERVAL {EVENT_TIME_OF_DAY_SECONDS} SECOND AS event_ts,
               CAST({d("c.received_date_key")} AS TIMESTAMP)
                   + INTERVAL {EVENT_TIME_OF_DAY_SECONDS} SECOND
                   + INTERVAL {int(cfg.outcome_lag_days)} DAY AS outcome_ts,
               c.claim_type, c.place_of_service, c.claim_status AS status, c.line_count,
               CAST(c.total_billed_amount AS DECIMAL(12,2)) AS billed,
               CAST(c.total_paid_amount AS DECIMAL(12,2)) AS paid,
               CAST(ROUND(c.total_billed_amount * 100) AS BIGINT) AS billed_c,
               dx.icd10_code AS icd10
        FROM {rd("fact_claim")} c
        JOIN {rd("dim_member")} m ON m.member_sk = c.member_sk
        JOIN {rd("dim_provider")} pr ON pr.provider_sk = c.provider_sk
        LEFT JOIN {rd("dim_diagnosis")} dx ON dx.diagnosis_sk = c.primary_diagnosis_sk
        ORDER BY c.claim_id
        """
    )
    for t, cols in (
        ("dim_member", "member_id, birth_year, sex, state, plan_type"),
        ("dim_provider", "provider_id, specialty, state, network_status, cost_tier"),
    ):
        con.execute(
            f"""
            CREATE OR REPLACE TABLE src_{t} AS
            SELECT {cols}, effective_from, effective_to FROM {rd(t)}
            ORDER BY {cols.split(",")[0]}, effective_from
            """
        )


def _check_scd2_unique(con, out: str, spine: str, what: str) -> None:
    n_out, n_sp = (con.execute(f"SELECT count(*) FROM {t}").fetchone()[0] for t in (out, spine))
    if n_out != n_sp:
        raise ValueError(
            f"{what}: SCD2 as-of join changed the row count ({n_sp} spine rows -> {n_out}); "
            "overlapping dimension versions (see docs/gold.md, SCD2 rules)"
        )


def build_member_features(con, n: Names, spine: str, out: str, cfg: FeatureConfig) -> None:
    """``spine(member_id, as_of)`` -> ``out`` with one row per spine row."""
    cond = {w: _w(w) for w in WINDOWS}
    flags = ",\n".join(
        f"coalesce(bool_or(regexp_matches(c.icd10, '{pat}')), false) AS has_{name}"
        for name, pat in CONDITIONS.items()
    )
    flag_sum = " + ".join(f"CAST(coalesce(a.has_{k}, false) AS INTEGER)" for k in CONDITIONS)
    flag_out = ",\n".join(f"coalesce(a.has_{k}, false) AS has_{k}" for k in CONDITIONS)
    out_cond = f"{cond[365]} AND {OUTCOME_COND}"
    con.execute(
        f"""
        CREATE OR REPLACE TABLE _agg_m AS
        SELECT s.member_id, s.as_of,
          count(*) FILTER (WHERE {cond[30]}) AS claims_30d,
          count(*) FILTER (WHERE {cond[90]}) AS claims_90d,
          count(*) FILTER (WHERE {cond[365]}) AS claims_365d,
          CAST(coalesce(sum(c.billed) FILTER (WHERE {cond[90]}), 0) AS DECIMAL(18,2))
              AS billed_sum_90d,
          CAST(coalesce(sum(c.billed) FILTER (WHERE {cond[365]}), 0) AS DECIMAL(18,2))
              AS billed_sum_365d,
          CAST(coalesce(sum(c.paid) FILTER (WHERE {out_cond}), 0) AS DECIMAL(18,2))
              AS paid_sum_365d,
          count(*) FILTER (WHERE {out_cond} AND c.status = '{DENIED}') AS denied_count_365d,
          count(*) FILTER (WHERE {out_cond}) AS _adj_365d,
          count(DISTINCT c.provider_id) FILTER (WHERE {cond[365]}) AS distinct_providers_365d,
          CAST(s.as_of AS DATE) - max(c.received_date) AS days_since_last_claim,
          {flags}
        FROM {spine} s JOIN {n.hist} c ON c.member_id = s.member_id AND {HIST_COND}
        GROUP BY s.member_id, s.as_of
        """
    )
    con.execute(
        f"""
        CREATE OR REPLACE TABLE {out} AS
        WITH j AS (
          SELECT s.member_id, s.as_of, d.plan_type, d.state,
                 CASE WHEN d.birth_year IS NULL THEN NULL
                      ELSE greatest(year(s.as_of) - d.birth_year, 0) END AS age
          FROM {spine} s LEFT JOIN {n.dim_member} d
            ON d.member_id = s.member_id AND {SCD2_COND}
        )
        SELECT j.member_id, j.as_of,
          coalesce(a.claims_30d, 0) AS claims_30d,
          coalesce(a.claims_90d, 0) AS claims_90d,
          coalesce(a.claims_365d, 0) AS claims_365d,
          coalesce(a.billed_sum_90d, 0) AS billed_sum_90d,
          coalesce(a.billed_sum_365d, 0) AS billed_sum_365d,
          coalesce(a.paid_sum_365d, 0) AS paid_sum_365d,
          coalesce(a.denied_count_365d, 0) AS denied_count_365d,
          CASE WHEN coalesce(a._adj_365d, 0) = 0 THEN NULL
               ELSE ROUND(a.denied_count_365d / CAST(a._adj_365d AS DOUBLE), 6) END
              AS denial_rate_365d,
          coalesce(a.distinct_providers_365d, 0) AS distinct_providers_365d,
          a.days_since_last_claim,
          {flag_out},
          {flag_sum} AS chronic_condition_count,
          j.plan_type, j.state,
          CASE WHEN j.age IS NULL THEN NULL WHEN j.age >= 90 THEN '90+'
               ELSE CAST(j.age - j.age % 5 AS VARCHAR) || '-' || CAST(j.age - j.age % 5 + 4 AS VARCHAR)
          END AS age_band
        FROM j LEFT JOIN _agg_m a ON a.member_id = j.member_id AND a.as_of = j.as_of
        ORDER BY j.member_id, j.as_of
        """
    )
    _check_scd2_unique(con, out, spine, "member features")


def build_provider_features(con, n: Names, spine: str, out: str, cfg: FeatureConfig) -> None:
    """``spine(provider_id, as_of)`` -> ``out``. Includes the peer-normalised cost z-score.

    Peers = providers with the same *as-of* specialty and at least ``min_peer_claims`` claims
    in 365d, computed over ALL providers (not just spine rows), so a row never depends on which
    other rows were requested. Provider mean billed per claim is held in integer cents (rounded
    half up) so the peer mean/variance are exact integer arithmetic (order independent).
    """
    cond = {w: _w(w) for w in WINDOWS}
    # all providers x distinct as_of: the peer universe
    con.execute(
        f"""
        CREATE OR REPLACE TABLE _peer_sp AS
        SELECT p.provider_id, a.as_of
        FROM (SELECT DISTINCT provider_id FROM {n.hist}) p
        CROSS JOIN (SELECT DISTINCT as_of FROM {spine}) a
        """
    )
    con.execute(
        f"""
        CREATE OR REPLACE TABLE _pm AS
        SELECT s.provider_id, s.as_of, count(*) AS n, sum(c.billed_c) AS sum_c,
               (2 * sum(c.billed_c) + count(*)) // (2 * count(*)) AS mean_c
        FROM _peer_sp s JOIN {n.hist} c ON c.provider_id = s.provider_id AND {HIST_COND}
             AND {cond[365]}
        GROUP BY s.provider_id, s.as_of
        """
    )
    con.execute(
        f"""
        CREATE OR REPLACE TABLE _peer AS
        SELECT s.as_of, d.specialty, count(*) AS k, sum(pm.mean_c) AS s1,
               sum(pm.mean_c * pm.mean_c) AS s2
        FROM _pm pm JOIN _peer_sp s ON s.provider_id = pm.provider_id AND s.as_of = pm.as_of
        JOIN {n.dim_provider} d ON d.provider_id = pm.provider_id AND {SCD2_COND}
        WHERE pm.n >= {int(cfg.min_peer_claims)}
        GROUP BY s.as_of, d.specialty
        """
    )
    con.execute(
        f"""
        CREATE OR REPLACE TABLE _agg_p AS
        SELECT s.provider_id, s.as_of,
          count(*) FILTER (WHERE {cond[90]}) AS claims_90d,
          count(*) AS claims_365d,
          count(DISTINCT c.member_id) AS distinct_members_365d,
          count(*) FILTER (WHERE {OUTCOME_COND}) AS _adj_365d,
          count(*) FILTER (WHERE {OUTCOME_COND} AND c.status = '{DENIED}') AS _den_365d,
          sum(c.billed_c) AS sum_c
        FROM {spine} s JOIN {n.hist} c ON c.provider_id = s.provider_id AND {HIST_COND}
             AND {cond[365]}
        GROUP BY s.provider_id, s.as_of
        """
    )
    con.execute(
        f"""
        CREATE OR REPLACE TABLE {out} AS
        WITH j AS (
          SELECT s.provider_id, s.as_of, d.specialty, d.network_status, d.cost_tier
          FROM {spine} s LEFT JOIN {n.dim_provider} d
            ON d.provider_id = s.provider_id AND {SCD2_COND}
        ),
        z AS (
          SELECT j.provider_id, j.as_of, a.claims_365d, a.sum_c,
                 (2 * a.sum_c + a.claims_365d) // (2 * a.claims_365d) AS mean_c,
                 pe.k, pe.s1, pe.s2
          FROM j JOIN _agg_p a ON a.provider_id = j.provider_id AND a.as_of = j.as_of
          LEFT JOIN _peer pe ON pe.as_of = j.as_of AND pe.specialty = j.specialty
        )
        SELECT j.provider_id, j.as_of,
          coalesce(a.claims_90d, 0) AS claims_90d,
          coalesce(a.claims_365d, 0) AS claims_365d,
          coalesce(a.distinct_members_365d, 0) AS distinct_members_365d,
          CASE WHEN coalesce(a._adj_365d, 0) = 0 THEN NULL
               ELSE ROUND(a._den_365d / CAST(a._adj_365d AS DOUBLE), 6) END AS denial_rate_365d,
          CASE WHEN a.claims_365d IS NULL THEN NULL
               ELSE ROUND(((2 * a.sum_c + a.claims_365d) // (2 * a.claims_365d)) / 100.0, 2)
          END AS avg_billed_365d,
          CASE WHEN z.claims_365d >= {int(cfg.min_peer_claims)} AND z.k >= {int(cfg.min_peers)}
                    AND z.k * z.s2 - z.s1 * z.s1 > 0
               THEN ROUND((z.mean_c - CAST(z.s1 AS DOUBLE) / z.k)
                    / sqrt(CAST(z.k * z.s2 - z.s1 * z.s1 AS DOUBLE) / (z.k * (z.k - 1))), 6)
          END AS cost_z_365d,
          j.specialty, j.network_status, j.cost_tier
        FROM j LEFT JOIN _agg_p a ON a.provider_id = j.provider_id AND a.as_of = j.as_of
        LEFT JOIN z ON z.provider_id = j.provider_id AND z.as_of = j.as_of
        ORDER BY j.provider_id, j.as_of
        """
    )
    _check_scd2_unique(con, out, spine, "provider features")


def claim_spine_sql(n: Names, as_of_filter: str | None = None) -> str:
    """One row per claim, as_of = start of the receipt day (the scoring moment)."""
    where = f"WHERE {as_of_filter}" if as_of_filter else ""
    return f"""
        SELECT claim_id, member_id, provider_id, CAST(received_date AS TIMESTAMP) AS as_of
        FROM {n.claims} {where}
    """


def build_claim_features(
    con, n: Names, out: str, cfg: FeatureConfig, as_of_filter: str | None = None
) -> None:
    """Claim-own attributes known at receipt + member (``m_``) and provider (``p_``) features."""
    con.execute(f"CREATE OR REPLACE TABLE _csp AS {claim_spine_sql(n, as_of_filter)}")
    con.execute("CREATE OR REPLACE TABLE _msp AS SELECT DISTINCT member_id, as_of FROM _csp")
    con.execute("CREATE OR REPLACE TABLE _psp AS SELECT DISTINCT provider_id, as_of FROM _csp")
    build_member_features(con, n, "_msp", "_mf", cfg)
    build_provider_features(con, n, "_psp", "_pf", cfg)
    mcols = [r[0] for r in con.execute("DESCRIBE _mf").fetchall()][2:]
    pcols = [r[0] for r in con.execute("DESCRIBE _pf").fetchall()][2:]
    msel = ", ".join(f"mf.{c} AS m_{c}" for c in mcols)
    psel = ", ".join(f"pf.{c} AS p_{c}" for c in pcols)
    con.execute(
        f"""
        CREATE OR REPLACE TABLE {out} AS
        SELECT s.claim_id, s.member_id, s.provider_id, s.as_of,
          c.billed AS billed_amount, c.line_count, c.claim_type, c.place_of_service,
          CAST(date_diff('day', c.service_date, c.received_date) AS INTEGER)
              AS service_to_received_lag_days,
          CASE WHEN c.icd10 IS NULL THEN NULL ELSE substr(c.icd10, 1, 1) END AS primary_dx_chapter,
          {msel}, {psel}
        FROM _csp s JOIN {n.claims} c ON c.claim_id = s.claim_id
        JOIN _mf mf ON mf.member_id = s.member_id AND mf.as_of = s.as_of
        JOIN _pf pf ON pf.provider_id = s.provider_id AND pf.as_of = s.as_of
        ORDER BY s.claim_id
        """
    )
