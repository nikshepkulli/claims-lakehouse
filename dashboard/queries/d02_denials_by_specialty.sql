-- question: Which provider specialties see the highest share of denied claims?
-- grain: one row per provider specialty (as of the service date), all months combined
-- caveat: Synthetic data; denial rates are generator parameters. Ratio of sums, not an average of monthly rates. The data has no denial reason code and no payer dimension, so "why" and "which payer" cannot be answered here; specialty and plan type are the closest available cuts. Small specialties give wide intervals.
-- columns: specialty, claim_count, denied_claim_count, denial_rate, denied_billed_amount
-- key_columns: specialty, claim_count, denied_claim_count, denial_rate
-- rows: one per specialty (about 8)
SELECT
    provider_specialty AS specialty,
    sum(claim_count) AS claim_count,
    sum(denied_claim_count) AS denied_claim_count,
    round(sum(denied_claim_count) / nullif(sum(claim_count), 0), 4) AS denial_rate,
    sum(denied_billed_amount) AS denied_billed_amount
FROM marts.mart_denials
GROUP BY provider_specialty
ORDER BY denial_rate DESC, specialty ASC
