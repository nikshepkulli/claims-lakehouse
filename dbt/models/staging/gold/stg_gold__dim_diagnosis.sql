SELECT
    diagnosis_sk,
    icd10_chapter,
    description AS diagnosis_description,
    loaded_at AS _loaded_at,
    upper(trim(icd10_code)) AS icd10_code
FROM {{ source('gold', 'dim_diagnosis') }}
