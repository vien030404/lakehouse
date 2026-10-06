{#
    Mốc cắt cho dữ liệu từ luồng stream.

    Airflow truyền hai biến, đều là thời gian thật theo ingest_ts (lúc Spark
    ghi sự kiện vào bronze):
        cutoff_ts       mốc của lần chốt này: chỉ lấy sự kiện có ingest_ts < mốc
        prev_cutoff_ts  mốc của lần chốt trước: sự kiện trong [prev, cutoff) là
                        phần thay đổi kể từ lần chốt trước
    Chạy dbt tay không truyền biến thì lấy toàn bộ sự kiện hiện có.

    Hai trục thời gian tách biệt: mốc cắt theo ingest_ts (thật), còn gold nhóm
    theo order_purchase_timestamp (thời gian mô phỏng của datagen).
#}

{% macro cutoff_filter(col='ingest_ts') -%}
    {%- if var('cutoff_ts', none) -%}
        {{ col }} < timestamp '{{ var("cutoff_ts") }}'
    {%- else -%}
        true
    {%- endif -%}
{%- endmacro %}


{#
    Thời điểm mô phỏng muộn nhất mà lần chốt này thấy được. Ngày mô phỏng
    chứa thời điểm này mới có một phần dữ liệu (phần còn lại đến ở lần chốt
    sau), nên gold chỉ công bố các ngày trước ngày này. Trả về NULL khi chưa
    có sự kiện stream nào.
#}
{% macro sim_cutoff() -%}
    (select max(sim_ts) from {{ source('bronze', 'order_events') }} where {{ cutoff_filter() }})
{%- endmacro %}


{# Điều kiện "ngày mô phỏng đã đóng" cho một cột thời gian mua hàng #}
{% macro closed_sim_days(col='order_purchase_timestamp') -%}
    {{ col }} < coalesce(date_trunc('day', {{ sim_cutoff() }}), timestamp '9999-12-31 00:00:00')
{%- endmacro %}
