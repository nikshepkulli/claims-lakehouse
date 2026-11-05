-- question: How do provider specialties compare on cost per claim, denial rate and claims per member?
-- grain: one row per provider specialty, all months combined
-- caveat: Synthetic data. Specialty is the provider's CURRENT SCD2 version. Ratios are recomputed from sums, not averaged from the monthly ratios. provider_count counts providers with at least one claim in the period.
-- columns: specialty, provider_count, claim_count, allowed_per_claim, denial_rate, paid_per_claim
-- key_columns: specialty, provider_count, claim_count, allowed_per_claim
-- rows: 12 specialties in the 2024 fixture
SELECT
    specialty,
    count(DISTINCT provider_id) AS provider_count,
    sum(claim_count) AS claim_count,
    round(sum(allowed_amount) / nullif(sum(claim_count), 0), 2) AS allowed_per_claim,
    round(sum(denied_claim_count) / nullif(sum(claim_count), 0), 4) AS denial_rate,
    round(sum(paid_amount) / nullif(sum(claim_count), 0), 2) AS paid_per_claim
FROM marts.mart_provider_performance
GROUP BY specialty
ORDER BY allowed_per_claim DESC
