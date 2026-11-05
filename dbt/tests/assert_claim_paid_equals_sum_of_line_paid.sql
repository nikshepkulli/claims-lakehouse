-- Reconciliation: each claim header's paid/allowed/billed total equals the sum of its
-- lines. Returns the claims that do not reconcile (test passes when zero rows).
WITH line_totals AS (
    SELECT
        claim_id,
        sum(paid_amount) AS paid,
        sum(allowed_amount) AS allowed,
        sum(billed_amount) AS billed
    FROM {{ ref('stg_gold__fact_claim_line') }}
    GROUP BY claim_id
)

SELECT
    c.claim_id,
    c.paid_amount AS header_paid,
    c.allowed_amount AS header_allowed,
    c.billed_amount AS header_billed,
    coalesce(l.paid, 0) AS lines_paid,
    coalesce(l.allowed, 0) AS lines_allowed,
    coalesce(l.billed, 0) AS lines_billed
FROM {{ ref('stg_gold__fact_claim') }} AS c
LEFT JOIN line_totals AS l ON c.claim_id = l.claim_id
WHERE
    c.paid_amount != coalesce(l.paid, 0)
    OR c.allowed_amount != coalesce(l.allowed, 0)
    OR c.billed_amount != coalesce(l.billed, 0)
