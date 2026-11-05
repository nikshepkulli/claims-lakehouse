-- question: Which 10 providers account for the most allowed dollars, and what cost tier are they in?
-- grain: one row per provider (ranked), summed over all service months
-- caveat: Synthetic provider ids. specialty and cost_tier are the CURRENT SCD2 version, so a provider whose tier changed is shown under its latest tier for all months. Allowed amount is the plan-recognised amount before member cost sharing, not what was paid.
-- columns: provider_id, specialty, cost_tier, claim_count, allowed_amount, avg_allowed_per_claim, provider_rank
-- key_columns: provider_id, specialty, cost_tier, allowed_amount, provider_rank
-- rows: exactly 10
WITH provider_totals AS (
    SELECT
        provider_id,
        max(specialty) AS specialty,
        max(cost_tier) AS cost_tier,
        sum(claim_count) AS claim_count,
        sum(allowed_amount) AS allowed_amount
    FROM marts.mart_provider_performance
    GROUP BY provider_id
)

SELECT
    provider_id,
    specialty,
    cost_tier,
    claim_count,
    allowed_amount,
    round(allowed_amount / nullif(claim_count, 0), 2) AS avg_allowed_per_claim,
    row_number() OVER (
        ORDER BY allowed_amount DESC, provider_id ASC
    ) AS provider_rank
FROM provider_totals
ORDER BY provider_rank
LIMIT 10
