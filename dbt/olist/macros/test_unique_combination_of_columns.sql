{#
    Generic test: tổ hợp các cột phải duy nhất (khoá tổ hợp).
    Tự cài đặt thay vì dùng dbt_utils để project không cần `dbt deps` và
    không phụ thuộc package bên ngoài.
#}
{% test unique_combination_of_columns(model, combination_of_columns) %}

    {%- set columns_csv = combination_of_columns | join(', ') -%}

    select
        {{ columns_csv }},
        count(*) as n_records
    from {{ model }}
    group by {{ columns_csv }}
    having count(*) > 1

{% endtest %}
