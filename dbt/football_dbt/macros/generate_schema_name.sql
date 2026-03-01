{% macro generate_schema_name(custom_schema_name, node) -%}
  {# 
    Force dbt to use the schema exactly as provided.
    If a model sets +schema: GOLD, schema will be GOLD (not SILVER_GOLD).
    If no custom schema is provided, fall back to target.schema (SILVER).
  #}
  {%- if custom_schema_name is none -%}
    {{ target.schema }}
  {%- else -%}
    {{ custom_schema_name }}
  {%- endif -%}
{%- endmacro %}