-- question: How is paid cost per member per month (PMPM) trending, and how much did it move vs the prior month?
-- grain: one row per service month
-- caveat: Synthetic data. member_months counts members enrolled by month end (current member versions only, no disenrollment), so PMPM is an approximation. Recent months can be understated while late-arriving claims (received 31-90 days after service) are still landing.
-- columns: service_month, member_months, paid_amount, pmpm_paid, pmpm_change_pct
-- key_columns: service_month, member_months, paid_amount, pmpm_paid
-- rows: 12 for the 2024 fixture; the first month has a NULL pmpm_change_pct (no prior month)
WITH monthly AS (
    SELECT
        service_month,
        member_months,
        paid_amount,
        pmpm_paid,
        lag(pmpm_paid) OVER (
            ORDER BY service_month
        ) AS prior_pmpm_paid
    FROM marts.mart_claims_monthly
)

SELECT
    service_month,
    member_months,
    paid_amount,
    pmpm_paid,
    round(100 * (pmpm_paid / prior_pmpm_paid - 1), 1) AS pmpm_change_pct
FROM monthly
ORDER BY service_month
