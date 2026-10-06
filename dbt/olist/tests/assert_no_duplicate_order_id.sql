-- =========================================================================
-- Singular test: trả về các order_id bị trùng trong bronze.orders.
-- Test fail khi query trả về ít nhất một dòng; n_duplicates cho biết mức độ.
-- Chạy riêng:  dbt test --select assert_no_duplicate_order_id
-- =========================================================================

select
    order_id,
    count(*) as n_duplicates
from {{ source('bronze', 'orders') }}
group by order_id
having count(*) > 1
