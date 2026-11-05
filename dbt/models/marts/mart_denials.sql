-- Grain: one row per service month x claim_type x provider specialty (specialty as of the
-- service date). denial_rate = denied claims / claims. denied_billed_amount is what the
-- provider billed on denied claims (paid is always 0 for denied claims).
WITH claims AS (
    SELECT * FROM {{ ref('int_claims_enriched') }}
)

SELECT
    service_month,
    cast(claim_type AS varchar) AS claim_type,
    cast(provider_specialty AS varchar) AS provider_specialty,
    cast(count(*) AS bigint) AS claim_count,
    cast(count(CASE WHEN is_denied THEN 1 END) AS bigint) AS denied_claim_count,
    cast(
        count(CASE WHEN is_denied THEN 1 END) / nullif(count(*), 0) AS numeric(9, 4)
    ) AS denial_rate,
    cast(sum(billed_amount) AS numeric(18, 2)) AS billed_amount,
    cast(sum(CASE WHEN is_denied THEN billed_amount ELSE 0 END) AS numeric(18, 2))
        AS denied_billed_amount
FROM claims
GROUP BY service_month, claim_type, provider_specialty
