{{ config(materialized = 'table') }}

-- =========================================================================
-- silver_order_facts ⋈ bronze.sellers ⋈ bronze.products.
-- Bảng điểm người bán: doanh thu, đánh giá, độ trễ giao hàng, xếp hạng theo
-- bang và nhóm tứ phân vị doanh thu.
-- =========================================================================

with facts as (

    select * from {{ ref('silver_order_facts') }}
    -- Chỉ đơn đã hoàn thành: luồng stream luôn có một lượng đơn đang dở (vừa
    -- tạo, đang giao); tính cả chúng sẽ làm điểm dao động theo thời điểm chốt
    -- thay vì phản ánh chất lượng người bán.
    where order_status in (
        {%- for s in var('completed_order_statuses') %}'{{ s }}'{% if not loop.last %}, {% endif %}{% endfor -%}
    )
      and {{ closed_sim_days() }}

),

sellers as (

    select
        seller_id,
        seller_state,
        seller_city
    from {{ source('bronze', 'sellers') }}

),

products as (

    select
        product_id,
        product_category_name
    from {{ source('bronze', 'products') }}

),

joined as (

    select
        f.seller_id,
        s.seller_state,
        s.seller_city,
        p.product_category_name,
        f.order_id,
        f.item_revenue,
        f.review_score_avg,
        f.delivery_delay_days
    from facts f
    join sellers s
      on f.seller_id = s.seller_id
    left join products p
      on f.product_id = p.product_id

),

scored as (

    select
        seller_id,
        seller_state,
        any_value(seller_city)                          as seller_city,
        count(distinct order_id)                        as orders,
        round(sum(item_revenue), 2)                     as revenue,
        round(avg(review_score_avg), 2)                 as avg_review_score,
        round(avg(delivery_delay_days), 1)              as avg_delay_days,
        count(distinct product_category_name)           as categories_sold
    from joined
    group by seller_id, seller_state

)

select
    *,
    ntile(4) over (order by revenue desc)               as revenue_quartile,
    rank() over (partition by seller_state order by revenue desc)
                                                        as rank_in_state
from scored
