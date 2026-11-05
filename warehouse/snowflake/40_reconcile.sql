-- Post-load reconciliation: each query should return zero rows / zero diff.
-- Read-only; safe to run as ANALYST or TRANSFORMER. ALL DATA IS SYNTHETIC.
-- Orphan foreign keys are expected only where the generator injected them
-- (docs/data-generator.md: orphan_member_id) and are quarantined upstream.

USE SCHEMA marts;

-- 1. Landed (deduplicated) vs loaded row counts per fact.
SELECT
    'fact_claim' AS table_name,
    (SELECT COUNT(DISTINCT claim_id) FROM raw.landing_fact_claim) AS landed_keys,
    (SELECT COUNT(*) FROM fact_claim) AS gold_rows
UNION ALL
SELECT
    'fact_claim_line' AS table_name,
    (SELECT COUNT(DISTINCT line_id) FROM raw.landing_fact_claim_line) AS landed_keys,
    (SELECT COUNT(*) FROM fact_claim_line) AS gold_rows;

-- 2. Claims whose member or provider surrogate key has no dimension row.
SELECT
    f.claim_id,
    f.member_sk,
    f.provider_sk
FROM fact_claim AS f
LEFT JOIN dim_member AS m ON f.member_sk = m.member_sk
LEFT JOIN dim_provider AS p ON f.provider_sk = p.provider_sk
WHERE m.member_sk IS NULL OR p.provider_sk IS NULL;

-- 3. Header totals must equal the sum of their lines (generator guarantee).
SELECT
    c.claim_id,
    c.total_paid_amount,
    SUM(l.paid_amount) AS line_paid_amount
FROM fact_claim AS c
INNER JOIN fact_claim_line AS l ON c.claim_id = l.claim_id
GROUP BY c.claim_id, c.total_paid_amount
HAVING c.total_paid_amount != SUM(l.paid_amount);

-- 4. SCD2 sanity: at most one current version per natural key.
SELECT member_id
FROM dim_member
WHERE is_current
GROUP BY member_id
HAVING COUNT(*) > 1;
