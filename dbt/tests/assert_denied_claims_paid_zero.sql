-- Business rule: a denied claim pays nothing. Returns violations.
SELECT
    claim_id,
    paid_amount
FROM {{ ref('stg_gold__fact_claim') }}
WHERE is_denied AND paid_amount != 0
