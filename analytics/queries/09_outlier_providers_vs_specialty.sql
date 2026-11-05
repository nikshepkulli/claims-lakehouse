-- question: Which high-volume providers cost well above their specialty average?
-- grain: one row per provider with at least 20 claims over the period (flagged when index >= 1.25)
-- caveat: Synthetic data; a flag is a prompt to look, not a finding about any provider. Index = provider allowed per claim / specialty allowed per claim (month by month in the mart, claims-weighted here), no case-mix or severity adjustment. Specialty and cost_tier are CURRENT SCD2 values.
-- columns: provider_id, specialty, cost_tier, claim_count, weighted_cost_index
-- key_columns: provider_id, specialty, cost_tier, claim_count, weighted_cost_index
-- rows: at least 1; ordered by index descending
SELECT
    provider_id,
    max(specialty) AS specialty,
    max(cost_tier) AS cost_tier,
    sum(claim_count) AS claim_count,
    round(sum(specialty_cost_index * claim_count) / sum(claim_count), 2) AS weighted_cost_index
FROM marts.mart_provider_performance
GROUP BY provider_id
HAVING
    sum(claim_count) >= 20
    AND sum(specialty_cost_index * claim_count) / sum(claim_count) >= 1.25
ORDER BY weighted_cost_index DESC, provider_id ASC
