-- SCD2 integrity for member and provider dimensions: exactly one current version per
-- natural key, and versions of a key never overlap. Returns violations.
WITH versions AS (
    SELECT
        'dim_member' AS dim,
        member_id AS natural_key,
        valid_from,
        valid_to,
        is_current
    FROM {{ ref('stg_gold__dim_member') }}
    UNION ALL
    SELECT
        'dim_provider' AS dim,
        provider_id AS natural_key,
        valid_from,
        valid_to,
        is_current
    FROM {{ ref('stg_gold__dim_provider') }}
),

current_counts AS (
    SELECT
        dim,
        natural_key,
        'current_versions_not_one' AS problem
    FROM versions
    GROUP BY dim, natural_key
    HAVING count(CASE WHEN is_current THEN 1 END) != 1
),

overlapping AS (
    SELECT
        a.dim,
        a.natural_key,
        'overlapping_versions' AS problem
    FROM versions AS a
    INNER JOIN versions AS b
        ON
            a.dim = b.dim
            AND a.natural_key = b.natural_key
            AND a.valid_from < b.valid_from
            AND (a.valid_to IS NULL OR a.valid_to > b.valid_from)
),

bad_current AS (
    SELECT
        dim,
        natural_key,
        'current_flag_disagrees_with_valid_to' AS problem
    FROM versions
    WHERE is_current != (valid_to IS NULL)
)

SELECT * FROM current_counts
UNION ALL
SELECT * FROM overlapping
UNION ALL
SELECT * FROM bad_current
