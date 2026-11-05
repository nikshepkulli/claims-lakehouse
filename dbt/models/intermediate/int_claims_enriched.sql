-- Grain: one row per claim. Joins the member and provider VERSION the gold fact
-- already points at, adds line aggregates and the readmission proxy flag.
WITH claims AS (
    SELECT * FROM {{ ref('stg_gold__fact_claim') }}
),

members AS (
    SELECT
        member_sk,
        member_id
    FROM {{ ref('stg_gold__dim_member') }}
),

providers AS (
    SELECT
        provider_sk,
        provider_id,
        specialty
    FROM {{ ref('stg_gold__dim_provider') }}
),

line_totals AS (
    SELECT
        claim_id,
        count(*) AS line_rows,
        sum(allowed_amount) AS line_allowed_amount,
        sum(paid_amount) AS line_paid_amount
    FROM {{ ref('stg_gold__fact_claim_line') }}
    GROUP BY claim_id
),

-- Readmission proxy: an institutional paid claim for a member whose previous
-- institutional paid claim was 0..N days earlier. There are no discharge dates
-- in the data, so this is a proxy on service dates, not a clinical readmission.
institutional AS (
    SELECT
        c.claim_id,
        datediff(
            'day',
            lag(c.service_date) OVER (
                PARTITION BY m.member_id
                ORDER BY c.service_date, c.claim_id
            ),
            c.service_date
        ) AS days_since_prior_institutional
    FROM claims AS c
    INNER JOIN members AS m ON c.member_sk = m.member_sk
    WHERE c.claim_type = 'INSTITUTIONAL' AND NOT c.is_denied
)

SELECT
    c.claim_id,
    m.member_id,
    p.provider_id,
    p.specialty AS provider_specialty,
    c.primary_diagnosis_sk,
    c.service_date,
    cast(date_trunc('month', c.service_date) AS date) AS service_month,
    c.received_date,
    c.claim_type,
    c.claim_status,
    c.is_denied,
    c.line_count,
    c.billed_amount,
    c.allowed_amount,
    c.paid_amount,
    coalesce(l.line_rows, 0) AS line_rows,
    coalesce(l.line_allowed_amount, 0) AS line_allowed_amount,
    coalesce(l.line_paid_amount, 0) AS line_paid_amount,
    coalesce(
        i.days_since_prior_institutional BETWEEN 0 AND {{ var('readmission_window_days') }}, false
    ) AS is_readmission_proxy
FROM claims AS c
INNER JOIN members AS m ON c.member_sk = m.member_sk
INNER JOIN providers AS p ON c.provider_sk = p.provider_sk
LEFT JOIN line_totals AS l ON c.claim_id = l.claim_id
LEFT JOIN institutional AS i ON c.claim_id = i.claim_id
