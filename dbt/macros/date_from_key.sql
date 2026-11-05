{#- YYYYMMDD integer date_key -> DATE. Plain string slicing, so it runs unchanged on
    DuckDB and Snowflake. -#}
{% macro date_from_key(column_name) -%}
    cast(
        substr(cast({{ column_name }} as varchar), 1, 4) || '-'
        || substr(cast({{ column_name }} as varchar), 5, 2) || '-'
        || substr(cast({{ column_name }} as varchar), 7, 2) as date
    )
{%- endmacro %}
