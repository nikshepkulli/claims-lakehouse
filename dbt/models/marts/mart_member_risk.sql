-- Grain: one row per member_id (current version), including members with no claims.
-- risk_score is a SYNTHETIC cost-percentile heuristic (0-100, percent_rank of total paid),
-- not an actuarial or clinical risk model. age_years is measured at the last service
-- date in the data, not today.
WITH members AS (
    SELECT
        member_id,
        birth_year,
        sex,
        state_code,
        plan_type,
        enrollment_date
    FROM {{ ref('stg_gold__dim_member') }}
    WHERE is_current
),

claims AS (
    SELECT * FROM {{ ref('int_claims_enriched') }}
),

as_of AS (
    SELECT max(service_date) AS as_of_date FROM claims
),

agg AS (
    SELECT
        member_id,
        count(*) AS claim_count,
        count(CASE WHEN is_denied THEN 1 END) AS denied_claim_count,
        count(CASE WHEN claim_type = 'INSTITUTIONAL' AND NOT is_denied THEN 1 END)
            AS institutional_claim_count,
        count(CASE WHEN is_readmission_proxy THEN 1 END) AS readmission_proxy_count,
        count(DISTINCT provider_id) AS distinct_provider_count,
        sum(allowed_amount) AS allowed_amount,
        sum(paid_amount) AS paid_amount,
        max(service_date) AS last_service_date
    FROM claims
    GROUP BY member_id
),

scored AS (
    SELECT
        m.member_id,
        m.birth_year,
        m.sex,
        m.state_code,
        m.plan_type,
        m.enrollment_date,
        a.last_service_date,
        coalesce(a.claim_count, 0) AS claim_count,
        coalesce(a.denied_claim_count, 0) AS denied_claim_count,
        coalesce(a.institutional_claim_count, 0) AS institutional_claim_count,
        coalesce(a.readmission_proxy_count, 0) AS readmission_proxy_count,
        coalesce(a.distinct_provider_count, 0) AS distinct_provider_count,
        coalesce(a.allowed_amount, 0) AS allowed_amount,
        coalesce(a.paid_amount, 0) AS paid_amount,
        year(o.as_of_date) - m.birth_year AS age_years,
        100 * percent_rank() OVER (
            ORDER BY coalesce(a.paid_amount, 0)
        ) AS risk_score
    FROM members AS m
    CROSS JOIN as_of AS o
    LEFT JOIN agg AS a ON m.member_id = a.member_id
)

SELECT
    cast(member_id AS varchar) AS member_id,
    cast(birth_year AS smallint) AS birth_year,
    cast(age_years AS smallint) AS age_years,
    cast(sex AS varchar) AS sex,
    cast(state_code AS varchar) AS state_code,
    cast(plan_type AS varchar) AS plan_type,
    enrollment_date,
    last_service_date,
    cast(claim_count AS bigint) AS claim_count,
    cast(denied_claim_count AS bigint) AS denied_claim_count,
    cast(institutional_claim_count AS bigint) AS institutional_claim_count,
    cast(readmission_proxy_count AS bigint) AS readmission_proxy_count,
    cast(distinct_provider_count AS bigint) AS distinct_provider_count,
    cast(allowed_amount AS numeric(18, 2)) AS allowed_amount,
    cast(paid_amount AS numeric(18, 2)) AS paid_amount,
    cast(risk_score AS numeric(5, 1)) AS risk_score,
    cast(
        CASE
            WHEN risk_score >= 90 THEN 'HIGH'
            WHEN risk_score >= 60 THEN 'MEDIUM'
            ELSE 'LOW'
        END AS varchar
    ) AS risk_tier
FROM scored
