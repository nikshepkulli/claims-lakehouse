SELECT
    line_id,
    claim_id,
    line_number,
    upper(trim(procedure_code)) AS procedure_code,
    units,
    {{ date_from_key('service_date_key') }} AS service_date,
    cast(billed_amount AS numeric(12, 2)) AS billed_amount,
    cast(allowed_amount AS numeric(12, 2)) AS allowed_amount,
    cast(paid_amount AS numeric(12, 2)) AS paid_amount,
    loaded_at AS _loaded_at
FROM {{ source('gold', 'fact_claim_line') }}
