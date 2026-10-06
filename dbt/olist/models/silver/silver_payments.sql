{{ config(materialized = 'table') }}

-- Gộp các lượt thanh toán (lịch sử + stream) về một dòng mỗi đơn, kèm
-- phương thức thanh toán có giá trị lớn nhất (chọn bằng window function).

with src as (

    select * from {{ source('bronze', 'payments') }}

    union all

    select order_id, payment_sequential, payment_type, payment_installments, payment_value
    from (
        select distinct event_id, order_id, payment_sequential, payment_type,
               payment_installments, payment_value
        from {{ source('bronze', 'payment_events') }}
        where {{ cutoff_filter() }}
    )

),

ranked as (

    select
        order_id,
        payment_type,
        payment_installments,
        payment_value,
        row_number() over (
            partition by order_id
            order by payment_value desc, payment_sequential asc
        ) as rn
    from src

),

aggregated as (

    select
        order_id,
        sum(payment_value)                  as payment_total,
        count(*)                            as payment_count,
        max(payment_installments)           as max_installments
    from src
    group by order_id

)

select
    a.order_id,
    a.payment_total,
    a.payment_count,
    a.max_installments,
    r.payment_type                          as primary_payment_type
from aggregated a
join ranked r
  on a.order_id = r.order_id
 and r.rn = 1
