SELECT
    member_sk,
    member_id,
    birth_year,
    enrollment_date,
    effective_from AS valid_from,
    effective_to AS valid_to,
    is_current,
    loaded_at AS _loaded_at,
    upper(trim(sex)) AS sex,
    upper(trim(state)) AS state_code,
    upper(trim(plan_type)) AS plan_type
FROM {{ source('gold', 'dim_member') }}
