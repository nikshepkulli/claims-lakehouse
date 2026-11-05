{{ config(materialized='table', schema='marts') }}

-- Day-grain time spine required by the MetricFlow semantic layer; reuses dim_date.
SELECT date_day
FROM {{ ref('stg_gold__dim_date') }}
