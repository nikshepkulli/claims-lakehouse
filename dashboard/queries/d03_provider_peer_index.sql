-- question: How does every sizeable provider's cost per claim compare with specialty peers?
-- grain: one row per provider with at least 20 claims over the period
-- caveat: Synthetic data; no case-mix or severity adjustment, so the index measures cost, not quality, and a flag is a prompt to look. Index = provider allowed per claim / specialty allowed per claim, claims-weighted across months (the mart's own normalisation). 1.0 = at specialty average. Specialty and cost_tier are the CURRENT SCD2 version.
-- columns: provider_id, specialty, cost_tier, claim_count, weighted_cost_index, denial_rate
-- key_columns: provider_id, specialty, cost_tier, claim_count, weighted_cost_index, denial_rate
-- rows: roughly 100 to 150 for the 5,000-claim fixture
SELECT
    provider_id,
    max(specialty) AS specialty,
    max(cost_tier) AS cost_tier,
    sum(claim_count) AS claim_count,
    round(sum(specialty_cost_index * claim_count) / sum(claim_count), 2) AS weighted_cost_index,
    round(sum(denied_claim_count) / sum(claim_count), 4) AS denial_rate
FROM marts.mart_provider_performance
GROUP BY provider_id
HAVING sum(claim_count) >= 20
ORDER BY weighted_cost_index DESC, provider_id ASC
