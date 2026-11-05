-- question: Does the denial rate differ across plan types?
-- grain: one row per plan_type
-- caveat: Synthetic data. mart_denials has no plan dimension, so this uses mart_member_risk, where plan_type is the member's CURRENT plan applied to all of their claims (SCD2: a member who switched plans is counted under the new plan only). Members with no claims add nothing to either side of the ratio.
-- columns: plan_type, member_count, claim_count, denied_claim_count, denial_rate
-- key_columns: plan_type, member_count, claim_count, denial_rate
-- rows: 6 plan types (HMO, PPO, EPO, HDHP, MEDICARE_ADV, MEDICAID_MCO)
SELECT
    plan_type,
    count(*) AS member_count,
    sum(claim_count) AS claim_count,
    sum(denied_claim_count) AS denied_claim_count,
    round(sum(denied_claim_count) / nullif(sum(claim_count), 0), 4) AS denial_rate
FROM marts.mart_member_risk
GROUP BY plan_type
ORDER BY denial_rate DESC
