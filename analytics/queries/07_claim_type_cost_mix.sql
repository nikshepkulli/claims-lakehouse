-- question: How does paid spend split between professional, institutional and pharmacy claims each month?
-- grain: one row per service month
-- caveat: Synthetic data. Shares are of paid_amount (denied claims pay 0, so they do not appear). The three claim-type paid columns sum to paid_amount by construction, so shares add to 1 up to rounding.
-- columns: service_month, paid_amount, professional_share, institutional_share, pharmacy_share
-- key_columns: service_month, paid_amount, professional_share, institutional_share, pharmacy_share
-- rows: 12 for the 2024 fixture
SELECT
    service_month,
    paid_amount,
    round(professional_paid_amount / nullif(paid_amount, 0), 4) AS professional_share,
    round(institutional_paid_amount / nullif(paid_amount, 0), 4) AS institutional_share,
    round(pharmacy_paid_amount / nullif(paid_amount, 0), 4) AS pharmacy_share
FROM marts.mart_claims_monthly
ORDER BY service_month
