-- question: How do utilisation and cost per member vary by age band, including members who never had a claim?
-- grain: one row per age band
-- caveat: Synthetic demographics. age_years is measured at the last service date in the data, not today. The member universe is current member versions, including members with zero claims, so averages are per enrolled member, not per claimant. Age bands are analyst-chosen, not a standard.
-- columns: age_band, member_count, members_with_claims, avg_claims_per_member, avg_paid_per_member
-- key_columns: age_band, member_count, avg_claims_per_member, avg_paid_per_member
-- rows: up to 5 age bands
SELECT
    CASE
        WHEN age_years < 18 THEN '00-17'
        WHEN age_years < 35 THEN '18-34'
        WHEN age_years < 50 THEN '35-49'
        WHEN age_years < 65 THEN '50-64'
        ELSE '65+'
    END AS age_band,
    count(*) AS member_count,
    count(CASE WHEN claim_count > 0 THEN 1 END) AS members_with_claims,
    round(avg(claim_count), 2) AS avg_claims_per_member,
    round(avg(paid_amount), 2) AS avg_paid_per_member
FROM marts.mart_member_risk
GROUP BY age_band
ORDER BY age_band
