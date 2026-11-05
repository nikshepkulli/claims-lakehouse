{#- Use the custom schema as-is (STAGING, MARTS) instead of <target schema>_<custom>.
    Matches the Snowflake schemas created by Terraform (docs/snowflake.md). -#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
