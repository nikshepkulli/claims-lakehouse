-- question: How concentrated is paid cost across members (Lorenz curve, one point per member percentile)?
-- grain: one row per member percentile bucket (1 to 100), members ranked from lowest to highest paid
-- caveat: Synthetic data; concentration is a generator parameter (skewed utilisation), not real population behaviour. All members count, including those with no claims. cum_paid_share is the share of total paid held by the lowest-paid members_pct percent of members. Paid is cost to the plan, not clinical risk.
-- columns: members_pct, cum_member_count, cum_paid_share
-- key_columns: members_pct, cum_member_count, cum_paid_share
-- rows: 100
WITH ranked AS (
    SELECT
        paid_amount,
        row_number() OVER (
            ORDER BY paid_amount ASC, member_id ASC
        ) AS rn,
        count(*) OVER () AS n,
        sum(paid_amount) OVER () AS total_paid
    FROM marts.mart_member_risk
),

cumulative AS (
    SELECT
        rn,
        n,
        total_paid,
        sum(paid_amount) OVER (
            ORDER BY rn
        ) AS cum_paid
    FROM ranked
)

SELECT
    ceil(100.0 * rn / n)::INTEGER AS members_pct,
    max(rn) AS cum_member_count,
    coalesce(round(max(cum_paid) / nullif(max(total_paid), 0), 6), 0) AS cum_paid_share
FROM cumulative
GROUP BY ceil(100.0 * rn / n)::INTEGER
ORDER BY members_pct
