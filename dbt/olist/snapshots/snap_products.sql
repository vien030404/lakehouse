{% snapshot snap_products %}

{#
    ---------------------------------------------------------------------------
    SCD Type 2 trên danh mục sản phẩm (nguồn: lakehouse.bronze.products).

    Đừng nhầm với Iceberg snapshot:
      - dbt snapshot  = bảng lịch sử SCD2 do dbt sinh ra, có dbt_valid_from /
                        dbt_valid_to, trả lời "thuộc tính sản phẩm lúc đó là gì".
      - Iceberg snapshot = phiên bản của cả bảng do engine ghi nhận, dùng cho
                        time travel (FOR VERSION AS OF) và expire_snapshots.

    Tạo một thay đổi để quan sát SCD2 (sửa bronze qua Trino rồi chạy lại snapshot):
      UPDATE lakehouse.bronze.products SET product_category_name = 'moveis_decoracao'
      WHERE product_id = '<một product_id bất kỳ>';
    Lần nạp lại bronze kế tiếp bằng Spark (ingest_bronze.py) ghi đè thay đổi
    này; snapshot khi đó thấy giá trị cũ quay lại và mở thêm một phiên bản.
    ---------------------------------------------------------------------------
#}

{{
    config(
        target_database = 'lakehouse',
        target_schema   = 'silver',
        unique_key      = 'product_id',
        strategy        = 'check',
        check_cols      = [
            'product_category_name',
            'product_weight_g',
            'product_length_cm',
            'product_height_cm',
            'product_width_cm'
        ],
        invalidate_hard_deletes = true
    )
}}

select
    product_id,
    product_category_name,
    product_name_lenght,
    product_description_lenght,
    product_photos_qty,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm
from {{ source('bronze', 'products') }}

{% endsnapshot %}
