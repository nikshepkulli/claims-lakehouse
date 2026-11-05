-- question: How often does a paid institutional claim follow another within 30 days, by month?
-- grain: one row per service month
-- caveat: This is a simplified PROXY on service dates (a paid institutional claim 0 to 30 days after the member's previous paid institutional claim), NOT a clinical readmission measure: the data has no discharge dates, no admission type or planned-readmission logic. The denominator here is all claims in the month because the marts do not expose an institutional claim count by month.
-- columns: service_month, readmission_proxy_count, claim_count, proxy_per_1000_claims
-- key_columns: service_month, readmission_proxy_count, claim_count, proxy_per_1000_claims
-- rows: 12 for the 2024 fixture
SELECT
    service_month,
    readmission_proxy_count,
    claim_count,
    round(1000 * readmission_proxy_count / nullif(claim_count, 0), 1) AS proxy_per_1000_claims
FROM marts.mart_claims_monthly
ORDER BY service_month
