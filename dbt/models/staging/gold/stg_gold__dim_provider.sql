SELECT
    provider_sk,
    provider_id,
    effective_from AS valid_from,
    effective_to AS valid_to,
    is_current,
    loaded_at AS _loaded_at,
    trim(specialty) AS specialty,
    upper(trim(state)) AS state_code,
    upper(trim(network_status)) AS network_status,
    upper(trim(cost_tier)) AS cost_tier
FROM {{ source('gold', 'dim_provider') }}
