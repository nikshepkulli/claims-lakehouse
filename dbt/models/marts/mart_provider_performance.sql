-- Grain: one row per provider_id per service month. Provider attributes are the CURRENT
-- version (is_current), so a provider appears once per month even if its SCD2 version
-- changed mid-month. specialty_cost_index = provider allowed per claim / specialty
-- allowed per claim for the same month (1.0 = at specialty average).
WITH claims AS (
    SELECT * FROM {{ ref('int_claims_enriched') }}
),

lines AS (
    SELECT
        c.provider_id,
        c.service_month,
        count(*) AS line_count
    FROM {{ ref('stg_gold__fact_claim_line') }} AS l
    INNER JOIN claims AS c ON l.claim_id = c.claim_id
    GROUP BY c.provider_id, c.service_month
),

providers AS (
    SELECT
        provider_id,
        specialty,
        state_code,
        network_status,
        cost_tier
    FROM {{ ref('stg_gold__dim_provider') }}
    WHERE is_current
),

agg AS (
    SELECT
        provider_id,
        service_month,
        count(*) AS claim_count,
        count(CASE WHEN is_denied THEN 1 END) AS denied_claim_count,
        count(DISTINCT member_id) AS unique_members,
        sum(allowed_amount) AS allowed_amount,
        sum(paid_amount) AS paid_amount,
        count(CASE WHEN is_readmission_proxy THEN 1 END) AS readmission_proxy_count
    FROM claims
    GROUP BY provider_id, service_month
),

joined AS (
    SELECT
        a.*,
        p.specialty,
        p.state_code,
        p.network_status,
        p.cost_tier,
        l.line_count,
        sum(a.allowed_amount) OVER (PARTITION BY p.specialty, a.service_month)
        / sum(a.claim_count) OVER (PARTITION BY p.specialty, a.service_month)
            AS specialty_allowed_per_claim
    FROM agg AS a
    INNER JOIN providers AS p ON a.provider_id = p.provider_id
    LEFT JOIN lines AS l ON a.provider_id = l.provider_id AND a.service_month = l.service_month
)

SELECT
    cast(provider_id AS varchar) AS provider_id,
    service_month,
    cast(specialty AS varchar) AS specialty,
    cast(state_code AS varchar) AS state_code,
    cast(network_status AS varchar) AS network_status,
    cast(cost_tier AS varchar) AS cost_tier,
    cast(claim_count AS bigint) AS claim_count,
    cast(denied_claim_count AS bigint) AS denied_claim_count,
    cast(unique_members AS bigint) AS unique_members,
    cast(coalesce(line_count, 0) AS bigint) AS line_count,
    cast(allowed_amount AS numeric(18, 2)) AS allowed_amount,
    cast(paid_amount AS numeric(18, 2)) AS paid_amount,
    cast(readmission_proxy_count AS bigint) AS readmission_proxy_count,
    cast(denied_claim_count / nullif(claim_count, 0) AS numeric(9, 4)) AS denial_rate,
    cast(allowed_amount / nullif(line_count, 0) AS numeric(18, 2)) AS avg_allowed_per_line,
    cast(paid_amount / nullif(claim_count, 0) AS numeric(18, 2)) AS avg_paid_per_claim,
    cast(
        (allowed_amount / nullif(claim_count, 0)) / nullif(specialty_allowed_per_claim, 0)
        AS numeric(9, 4)
    ) AS specialty_cost_index
FROM joined
