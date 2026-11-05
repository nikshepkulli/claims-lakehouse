-- The header's line_count equals the number of line rows. Returns mismatches.
SELECT
    c.claim_id,
    c.line_count,
    count(l.line_id) AS actual_lines
FROM {{ ref('stg_gold__fact_claim') }} AS c
LEFT JOIN {{ ref('stg_gold__fact_claim_line') }} AS l ON c.claim_id = l.claim_id
GROUP BY c.claim_id, c.line_count
HAVING c.line_count != count(l.line_id)
