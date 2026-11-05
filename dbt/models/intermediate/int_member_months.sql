-- Grain: one row per calendar month. member_months = members enrolled on or before
-- the last day of the month (current member versions only; enrollment is open-ended
-- in the synthetic data, no disenrollment). Null enrollment_date counts as enrolled.
WITH months AS (
    SELECT DISTINCT cast(date_trunc('month', date_day) AS date) AS service_month
    FROM {{ ref('stg_gold__dim_date') }}
),

members AS (
    SELECT
        member_id,
        enrollment_date
    FROM {{ ref('stg_gold__dim_member') }}
    WHERE is_current
)

SELECT
    months.service_month,
    count(members.member_id) AS member_months
FROM months
LEFT JOIN members
    ON members.enrollment_date IS NULL OR members.enrollment_date <= last_day(months.service_month)
GROUP BY months.service_month
