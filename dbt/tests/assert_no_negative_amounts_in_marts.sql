-- No negative money in any mart. Returns offending mart rows.
SELECT
    'mart_claims_monthly' AS mart,
    cast(month_key AS varchar) AS record_id
FROM {{ ref('mart_claims_monthly') }}
WHERE
    billed_amount < 0 OR allowed_amount < 0 OR paid_amount < 0
    OR pmpm_paid < 0 OR avg_allowed_per_line < 0
UNION ALL
SELECT
    'mart_provider_performance' AS mart,
    provider_id || '|' || cast(service_month AS varchar) AS record_id
FROM {{ ref('mart_provider_performance') }}
WHERE allowed_amount < 0 OR paid_amount < 0 OR avg_paid_per_claim < 0
UNION ALL
SELECT
    'mart_member_risk' AS mart,
    member_id AS record_id
FROM {{ ref('mart_member_risk') }}
WHERE allowed_amount < 0 OR paid_amount < 0
UNION ALL
SELECT
    'mart_denials' AS mart,
    cast(service_month AS varchar) || '|' || claim_type AS record_id
FROM {{ ref('mart_denials') }}
WHERE billed_amount < 0 OR denied_billed_amount < 0
