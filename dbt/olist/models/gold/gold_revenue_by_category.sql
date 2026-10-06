{{ config(materialized = 'table') }}

-- =========================================================================
-- silver_order_facts ⋈ bronze.products ⋈ bronze.category_translation.
-- Doanh thu theo nhóm hàng và tháng, tên nhóm dịch sang tiếng Anh, kèm tỉ
-- trọng doanh thu trong tháng.
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

products as (

    select
        product_id,
        product_category_name,
        product_weight_g,
        product_photos_qty
    from {{ source('bronze', 'products') }}

),

translation as (

    select
        product_category_name,
        product_category_name_english
    from {{ source('bronze', 'category_translation') }}

),

joined as (

    select
        coalesce(t.product_category_name_english, p.product_category_name, 'unknown')
                                            as product_category,
        f.order_month,
        f.order_id,
        f.product_id,
        f.item_revenue,
        f.price,
        f.freight_value,
        f.review_score_avg,
        p.product_weight_g
    from facts f
    left join products p
      on f.product_id = p.product_id
    left join translation t
      on p.product_category_name = t.product_category_name

)

select
    product_category,
    order_month,
    count(distinct order_id)                        as orders,
    count(*)                                        as items_sold,
    round(sum(item_revenue), 2)                     as revenue,
    round(avg(price), 2)                            as avg_item_price,
    round(sum(freight_value) / nullif(sum(item_revenue), 0), 4)
                                                    as freight_ratio,
    round(avg(review_score_avg), 2)                 as avg_review_score,
    round(avg(product_weight_g), 0)                 as avg_weight_g,

    -- tỉ trọng doanh thu của nhóm hàng trong tháng
    round(
        100.0 * sum(item_revenue)
        / nullif(sum(sum(item_revenue)) over (partition by order_month), 0)
    , 2)                                            as revenue_share_pct,

    -- Tháng có số liệu cuối cùng: đã qua hết tháng + 30 ngày mô phỏng
    coalesce(order_month + interval '1' month + interval '30' day <= {{ sim_cutoff() }}, true)   as is_final

from joined
group by product_category, order_month
