-- Totals in the marts equal totals in staging (no claim lost or double counted in joins).
-- Returns a row per metric that does not match.
WITH stg AS (
    SELECT
        count(*) AS claims,
        sum(paid_amount) AS paid
    FROM {{ ref('stg_gold__fact_claim') }}
),

checks AS (
    SELECT
        'mart_claims_monthly.paid' AS metric,
        (SELECT sum(paid_amount) FROM {{ ref('mart_claims_monthly') }}) AS mart_value,
        (SELECT paid FROM stg) AS stg_value
    UNION ALL
    SELECT
        'mart_claims_monthly.claims' AS metric,
        (SELECT sum(claim_count) FROM {{ ref('mart_claims_monthly') }}) AS mart_value,
        (SELECT claims FROM stg) AS stg_value
    UNION ALL
    SELECT
        'mart_provider_performance.paid' AS metric,
        (SELECT sum(paid_amount) FROM {{ ref('mart_provider_performance') }}) AS mart_value,
        (SELECT paid FROM stg) AS stg_value
    UNION ALL
    SELECT
        'mart_member_risk.paid' AS metric,
        (SELECT sum(paid_amount) FROM {{ ref('mart_member_risk') }}) AS mart_value,
        (SELECT paid FROM stg) AS stg_value
    UNION ALL
    SELECT
        'mart_denials.claims' AS metric,
        (SELECT sum(claim_count) FROM {{ ref('mart_denials') }}) AS mart_value,
        (SELECT claims FROM stg) AS stg_value
)

SELECT *
FROM checks
WHERE mart_value != stg_value
