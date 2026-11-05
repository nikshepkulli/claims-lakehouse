SELECT
    claim_id,
    member_sk,
    provider_sk,
    primary_diagnosis_sk,
    {{ date_from_key('service_date_key') }} AS service_date,
    {{ date_from_key('received_date_key') }} AS received_date,
    upper(trim(claim_type)) AS claim_type,
    place_of_service,
    upper(trim(claim_status)) AS claim_status,
    upper(trim(claim_status)) = 'DENIED' AS is_denied,
    line_count,
    cast(total_billed_amount AS numeric(12, 2)) AS billed_amount,
    cast(total_allowed_amount AS numeric(12, 2)) AS allowed_amount,
    cast(total_paid_amount AS numeric(12, 2)) AS paid_amount,
    loaded_at AS _loaded_at
FROM {{ source('gold', 'fact_claim') }}
