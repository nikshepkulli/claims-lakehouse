-- question: How concentrated is spend: what do HIGH risk-tier members cost, and which plans do they sit in?
-- grain: one row per plan_type x risk_tier, restricted to the HIGH and MEDIUM tiers
-- caveat: risk_tier is a cost-percentile heuristic on synthetic data (top 10 percent of members by total paid = HIGH), NOT a clinical or actuarial risk model, and it is based on the same paid dollars it is used to explain. Plan is the current SCD2 version. Members with no claims are LOW and excluded here.
-- columns: plan_type, risk_tier, member_count, paid_amount, avg_paid_per_member, share_of_total_paid
-- key_columns: plan_type, risk_tier, member_count, paid_amount
-- rows: up to 12 (6 plans x 2 tiers)
SELECT
    plan_type,
    risk_tier,
    count(*) AS member_count,
    sum(paid_amount) AS paid_amount,
    round(sum(paid_amount) / count(*), 2) AS avg_paid_per_member,
    round(sum(paid_amount) / sum(sum(paid_amount)) OVER (), 4) AS share_of_total_paid
FROM marts.mart_member_risk
WHERE risk_tier IN ('HIGH', 'MEDIUM')
GROUP BY plan_type, risk_tier
ORDER BY plan_type, risk_tier
