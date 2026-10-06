{#
    Mặc định dbt ghép <schema của profile>_<schema custom> (ví dụ silver_gold).
    Macro này dùng đúng tên schema khai trong dbt_project.yml: silver, gold.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
