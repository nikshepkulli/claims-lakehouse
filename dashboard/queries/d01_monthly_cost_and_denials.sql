-- question: How do billed, allowed and paid cost, PMPM and the denial rate move month by month?
-- grain: one row per service month
-- caveat: Synthetic data. Billed is what providers asked for, paid is what the plan paid (0 on denials), so paid_to_billed mixes the allowed-to-billed discount with denials. PMPM uses an approximate member-month denominator (no disenrollment). Late-arriving claims can still lift recent months.
-- columns: service_month, claim_count, denied_claim_count, member_months, billed_amount, allowed_amount, paid_amount, pmpm_paid, denial_rate, paid_to_billed
-- key_columns: service_month, claim_count, denied_claim_count, member_months, billed_amount, allowed_amount, paid_amount, pmpm_paid
-- rows: 12 for the 2024 fixture
SELECT
    service_month,
    claim_count,
    denied_claim_count,
    member_months,
    billed_amount,
    allowed_amount,
    paid_amount,
    pmpm_paid,
    round(denied_claim_count / nullif(claim_count, 0), 4) AS denial_rate,
    round(paid_amount / nullif(billed_amount, 0), 4) AS paid_to_billed
FROM marts.mart_claims_monthly
ORDER BY service_month
