-- The generator injects negative amounts (negative_amount defect). Gold must have
-- quarantined them; if any reach staging this test fails loudly instead of the
-- models silently filtering them. Returns offending rows.
SELECT
    'fact_claim' AS source_table,
    claim_id AS record_id
FROM {{ ref('stg_gold__fact_claim') }}
WHERE billed_amount < 0 OR allowed_amount < 0 OR paid_amount < 0
UNION ALL
SELECT
    'fact_claim_line' AS source_table,
    line_id AS record_id
FROM {{ ref('stg_gold__fact_claim_line') }}
WHERE billed_amount < 0 OR allowed_amount < 0 OR paid_amount < 0
