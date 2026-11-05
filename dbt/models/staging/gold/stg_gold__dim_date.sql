SELECT
    date_key,
    full_date AS date_day,
    year AS calendar_year,
    quarter AS calendar_quarter,
    month AS calendar_month,
    month_name,
    day_of_month,
    day_of_week,
    is_weekend
FROM {{ source('gold', 'dim_date') }}
