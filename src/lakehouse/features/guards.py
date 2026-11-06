"""Leakage guards. ``build`` runs them and fails (non-zero exit) if any trips.

Static guard: no label/outcome column may appear in a feature table (``assert_label_separation``).

Dynamic guard (``run_leakage_guard``): for probe moments ``A`` it recomputes the member, provider
and claim features at ``A`` three ways and demands identical output:

1. baseline: all data;
2. *truncated*: history rows with event time > A and dimension versions with effective_from > A
   removed (what a time machine would have shown at A);
3. *mutated*: every value that was not yet knowable at A is scrambled (outcomes of claims whose
   outcome lands after A, everything about claims received after A, dimension versions starting
   after A). Own-claim fields that ARE legitimate features (billed, type, dx) are left alone, so
   the only way the claim features change is by reading an outcome or a future row.

Any difference means a feature reads something it should not have, e.g. a dropped upper bound on
the history join, an outcome used before its lag, or a claim's own label.
"""

# ruff: noqa: E501  (embedded SQL)
from __future__ import annotations

from dataclasses import replace
from datetime import datetime

from .config import FeatureConfig
from .sql import Names, build_claim_features, build_member_features, build_provider_features

# Columns that carry the label or an outcome of the claim being scored. Never in a feature table.
FORBIDDEN_FEATURE_COLUMNS = frozenset(
    {
        "is_denied",
        "claim_status",
        "status",
        "label",
        "denied",
        "total_paid_amount",
        "total_allowed_amount",
        "paid_amount",
        "allowed_amount",
        "paid",
        "allowed",
    }
)


class LeakageError(AssertionError):
    """A feature depends on information that was not available at its as_of."""


def assert_label_separation(columns: list[str], table: str = "feature table") -> None:
    bad = sorted(c for c in columns if c.lower() in FORBIDDEN_FEATURE_COLUMNS)
    if bad:
        raise LeakageError(f"{table} contains label/outcome columns {bad}")


def probe_moments(con, n: int = 4) -> list[datetime]:
    """Evenly spaced distinct receipt-day starts of the data, deterministic."""
    days = [
        r[0]
        for r in con.execute(
            "SELECT DISTINCT CAST(received_date AS TIMESTAMP) FROM src_claims ORDER BY 1"
        ).fetchall()
    ]
    if len(days) <= n:
        return days
    step = (len(days) - 1) / (n - 1) if n > 1 else 0
    return [days[round(i * step)] for i in range(n)]


def _perturb(con, a: datetime) -> Names:
    """Create the truncated and mutated copies of the sources for probe moment ``a``."""
    ts = f"TIMESTAMP '{a:%Y-%m-%d %H:%M:%S}'"
    con.execute(
        f"CREATE OR REPLACE TABLE g_trunc AS SELECT * FROM src_claims WHERE event_ts <= {ts}"
    )
    con.execute(
        f"""
        CREATE OR REPLACE TABLE g_mut AS
        SELECT claim_id,
          CASE WHEN event_ts > {ts} AND received_date <> CAST({ts} AS DATE)
               THEN 'GHOST' || member_id ELSE member_id END AS member_id,
          CASE WHEN event_ts > {ts} AND received_date <> CAST({ts} AS DATE)
               THEN 'GHOST' || provider_id ELSE provider_id END AS provider_id,
          service_date, received_date, event_ts, outcome_ts, claim_type, place_of_service,
          CASE WHEN outcome_ts > {ts}
               THEN CASE status WHEN 'DENIED' THEN 'PAID' ELSE 'DENIED' END ELSE status END AS status,
          line_count,
          CASE WHEN event_ts > {ts} AND received_date <> CAST({ts} AS DATE)
               THEN billed * 3 ELSE billed END AS billed,
          CASE WHEN outcome_ts > {ts} THEN paid + 12345.67 ELSE paid END AS paid,
          CASE WHEN event_ts > {ts} AND received_date <> CAST({ts} AS DATE)
               THEN billed_c * 3 ELSE billed_c END AS billed_c,
          CASE WHEN event_ts > {ts} AND received_date <> CAST({ts} AS DATE)
               THEN 'I10' ELSE icd10 END AS icd10
        FROM src_claims
        """
    )
    for t in ("dim_member", "dim_provider"):
        con.execute(
            f"CREATE OR REPLACE TABLE g_trunc_{t} AS SELECT * FROM src_{t} WHERE effective_from <= {ts}"
        )
    con.execute(
        f"""
        CREATE OR REPLACE TABLE g_mut_dim_member AS
        SELECT member_id, birth_year, sex,
               CASE WHEN effective_from > {ts} THEN 'LEAK' ELSE state END AS state,
               CASE WHEN effective_from > {ts} THEN 'LEAK' ELSE plan_type END AS plan_type,
               effective_from, effective_to FROM src_dim_member
        """
    )
    con.execute(
        f"""
        CREATE OR REPLACE TABLE g_mut_dim_provider AS
        SELECT provider_id,
               CASE WHEN effective_from > {ts} THEN 'LEAK' ELSE specialty END AS specialty,
               state, network_status,
               CASE WHEN effective_from > {ts} THEN 'LEAK' ELSE cost_tier END AS cost_tier,
               effective_from, effective_to FROM src_dim_provider
        """
    )
    return ts


def _features_at(con, n: Names, ts: str, cfg: FeatureConfig, tag: str) -> dict[str, str]:
    """Member, provider and claim feature tables at the single moment ``ts``."""
    con.execute(
        f"CREATE OR REPLACE TABLE _gm AS SELECT DISTINCT member_id, {ts} AS as_of FROM src_dim_member"
    )
    con.execute(
        f"CREATE OR REPLACE TABLE _gp AS SELECT DISTINCT provider_id, {ts} AS as_of FROM src_dim_provider"
    )
    build_member_features(con, n, "_gm", f"g_m_{tag}", cfg)
    build_provider_features(con, n, "_gp", f"g_p_{tag}", cfg)
    build_claim_features(
        con, n, f"g_c_{tag}", cfg, as_of_filter=f"CAST(received_date AS TIMESTAMP) = {ts}"
    )
    return {"member": f"g_m_{tag}", "provider": f"g_p_{tag}", "claim": f"g_c_{tag}"}


def _diff(con, a: str, b: str) -> tuple[int, list]:
    q = f"(SELECT * FROM {a} EXCEPT SELECT * FROM {b}) UNION ALL (SELECT * FROM {b} EXCEPT SELECT * FROM {a})"
    rows = con.execute(f"SELECT * FROM ({q}) LIMIT 3").fetchall()
    n = con.execute(f"SELECT count(*) FROM ({q})").fetchone()[0]
    return n, rows


def run_leakage_guard(con, cfg: FeatureConfig, probes: int = 4) -> dict:
    """Raise :class:`LeakageError` if any probe moment shows a difference. Returns a summary."""
    base = Names()
    checked = 0
    for a in probe_moments(con, probes):
        ts = _perturb(con, a)
        full = _features_at(con, base, ts, cfg, "full")
        variants = {
            "truncated": replace(
                base,
                hist="g_trunc",
                dim_member="g_trunc_dim_member",
                dim_provider="g_trunc_dim_provider",
            ),
            "mutated": Names(
                claims="g_mut",
                hist="g_mut",
                dim_member="g_mut_dim_member",
                dim_provider="g_mut_dim_provider",
            ),
        }
        for vname, names in variants.items():
            alt = _features_at(con, names, ts, cfg, vname)
            for entity, t in full.items():
                n_diff, sample = _diff(con, t, alt[entity])
                checked += 1
                if n_diff:
                    raise LeakageError(
                        f"{entity} features at as_of={a:%Y-%m-%d %H:%M:%S} change when "
                        f"{'future rows are removed' if vname == 'truncated' else 'unknowable values are scrambled'}"
                        f" ({n_diff} differing rows, e.g. {sample})"
                    )
    return {"probes": probes, "comparisons": checked}
