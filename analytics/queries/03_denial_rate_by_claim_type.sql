-- question: What share of claims is denied for each claim type, and how much billed value sits in those denials?
-- grain: one row per claim_type, all months combined
-- caveat: Synthetic data; denial rates are generator parameters, not real payer behaviour. The rate is recomputed as a ratio of sums (denied claims / claims). Averaging the monthly denial_rate column instead would weight tiny cells like a large one. denied_billed_amount is billed, not paid (paid is always 0 on denials).
-- columns: claim_type, claim_count, denied_claim_count, denial_rate, denied_billed_amount
-- key_columns: claim_type, claim_count, denial_rate
-- rows: 3 (PHARMACY, INSTITUTIONAL, PROFESSIONAL)
SELECT
    claim_type,
    sum(claim_count) AS claim_count,
    sum(denied_claim_count) AS denied_claim_count,
    round(sum(denied_claim_count) / nullif(sum(claim_count), 0), 4) AS denial_rate,
    sum(denied_billed_amount) AS denied_billed_amount
FROM marts.mart_denials
GROUP BY claim_type
ORDER BY denial_rate DESC
