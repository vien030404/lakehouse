{{ config(materialized = 'table') }}

-- =========================================================================
-- silver_order_facts ⋈ silver_customers (lịch sử Olist + khách từ luồng stream).
-- Doanh thu theo bang của khách và tháng, xếp hạng và luỹ kế bằng window function.
-- Bảng này có row filter trong trino/security/rules.json (analyst chỉ thấy 'SP').
-- =========================================================================

with facts as (

    select *
    from {{ ref('silver_order_facts') }}
    where order_status in (
        {%- for s in var('revenue_order_statuses') %}'{{ s }}'{% if not loop.last %}, {% endif %}{% endfor -%}
    )
      -- chỉ ngày mô phỏng đã đóng; ngày đang diễn ra vào ở lần chốt sau
      and {{ closed_sim_days() }}

),

customers as (

    select
        customer_id,
        customer_unique_id,
        customer_state,
        customer_city
    from {{ ref('silver_customers') }}

),

joined as (

    select
        c.customer_state,
        f.order_month,
        f.order_id,
        f.customer_id,
        c.customer_unique_id,
        f.item_revenue,
        f.freight_value,
        f.delivery_days
    from facts f
    join customers c
      on f.customer_id = c.customer_id

),

aggregated as (

    select
        customer_state,
        order_month,
        count(distinct order_id)            as orders,
        count(distinct customer_unique_id)  as buyers,
        sum(item_revenue)                   as revenue,
        sum(freight_value)                  as freight,
        avg(delivery_days)                  as avg_delivery_days
    from joined
    group by customer_state, order_month

)

select
    customer_state,
    order_month,
    orders,
    buyers,
    round(revenue, 2)                                   as revenue,
    round(freight, 2)                                   as freight,
    round(avg_delivery_days, 1)                         as avg_delivery_days,
    round(revenue / nullif(orders, 0), 2)               as avg_order_value,

    -- window function: xếp hạng bang theo doanh thu trong từng tháng
    rank() over (
        partition by order_month order by revenue desc
    )                                                   as revenue_rank_in_month,

    -- window function: doanh thu luỹ kế của bang theo thời gian
    round(sum(revenue) over (
        partition by customer_state
        order by order_month
        rows between unbounded preceding and current row
    ), 2)                                               as revenue_running_total,

    -- Tháng có số liệu cuối cùng: đã qua hết tháng + 30 ngày mô phỏng, đủ để
    -- các đơn trong tháng giao xong. Tháng chưa cuối cùng còn tăng ở các lần
    -- chốt sau.
    coalesce(order_month + interval '1' month + interval '30' day <= {{ sim_cutoff() }}, true)   as is_final

from aggregated
