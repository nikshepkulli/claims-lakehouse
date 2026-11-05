-- SCD2 as-of correctness: the member/provider version a claim points at must be valid on
-- the claim's service date. Returns claims pointing at a wrong version.
SELECT
    c.claim_id,
    c.service_date
FROM {{ ref('stg_gold__fact_claim') }} AS c
INNER JOIN {{ ref('stg_gold__dim_member') }} AS m ON c.member_sk = m.member_sk
INNER JOIN {{ ref('stg_gold__dim_provider') }} AS p ON c.provider_sk = p.provider_sk
WHERE
    c.service_date < cast(m.valid_from AS date)
    OR (m.valid_to IS NOT NULL AND c.service_date >= cast(m.valid_to AS date))
    OR c.service_date < cast(p.valid_from AS date)
    OR (p.valid_to IS NOT NULL AND c.service_date >= cast(p.valid_to AS date))
