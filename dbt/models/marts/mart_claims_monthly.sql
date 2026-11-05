-- Grain: one row per service month (all claim types). Additive columns sum across months;
-- member_months is the month's enrolled-member count and must not be summed across
-- breakdowns. PMPM = paid_amount / member_months.
WITH claims AS (
    SELECT * FROM {{ ref('int_claims_enriched') }}
),

line_counts AS (
    SELECT
        cast(date_trunc('month', service_date) AS date) AS service_month,
        count(*) AS line_count,
        sum(allowed_amount) AS line_allowed_amount
    FROM {{ ref('stg_gold__fact_claim_line') }}
    GROUP BY service_month
),

monthly AS (
    SELECT
        service_month,
        cast(year(service_month) * 100 + month(service_month) AS integer) AS month_key,
        count(*) AS claim_count,
        count(CASE WHEN is_denied THEN 1 END) AS denied_claim_count,
        count(DISTINCT member_id) AS active_member_count,
        sum(billed_amount) AS billed_amount,
        sum(allowed_amount) AS allowed_amount,
        sum(paid_amount) AS paid_amount,
        sum(CASE WHEN claim_type = 'PROFESSIONAL' THEN paid_amount ELSE 0 END)
            AS professional_paid_amount,
        sum(CASE WHEN claim_type = 'INSTITUTIONAL' THEN paid_amount ELSE 0 END)
            AS institutional_paid_amount,
        sum(CASE WHEN claim_type = 'PHARMACY' THEN paid_amount ELSE 0 END)
            AS pharmacy_paid_amount,
        count(CASE WHEN is_readmission_proxy THEN 1 END) AS readmission_proxy_count
    FROM claims
    GROUP BY service_month, month_key
)

SELECT
    m.month_key,
    m.service_month,
    cast(m.claim_count AS bigint) AS claim_count,
    cast(m.denied_claim_count AS bigint) AS denied_claim_count,
    cast(m.active_member_count AS bigint) AS active_member_count,
    cast(mm.member_months AS bigint) AS member_months,
    cast(l.line_count AS bigint) AS line_count,
    cast(m.billed_amount AS numeric(18, 2)) AS billed_amount,
    cast(m.allowed_amount AS numeric(18, 2)) AS allowed_amount,
    cast(m.paid_amount AS numeric(18, 2)) AS paid_amount,
    cast(m.professional_paid_amount AS numeric(18, 2)) AS professional_paid_amount,
    cast(m.institutional_paid_amount AS numeric(18, 2)) AS institutional_paid_amount,
    cast(m.pharmacy_paid_amount AS numeric(18, 2)) AS pharmacy_paid_amount,
    cast(m.readmission_proxy_count AS bigint) AS readmission_proxy_count,
    cast(m.paid_amount / nullif(mm.member_months, 0) AS numeric(18, 2)) AS pmpm_paid,
    cast(m.denied_claim_count / nullif(m.claim_count, 0) AS numeric(9, 4)) AS denial_rate,
    cast(l.line_allowed_amount / nullif(l.line_count, 0) AS numeric(18, 2)) AS avg_allowed_per_line
FROM monthly AS m
INNER JOIN {{ ref('int_member_months') }} AS mm ON m.service_month = mm.service_month
LEFT JOIN line_counts AS l ON m.service_month = l.service_month
