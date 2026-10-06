-- =========================================================================
-- Singular test đối soát: tổng doanh thu ở gold_revenue_by_state phải khớp
-- tổng doanh thu cùng phạm vi ở silver_order_facts. Lệch nhau nghĩa là có
-- dòng hàng bị rơi khi join sang silver_customers.
-- =========================================================================

with by_state as (
    select sum(revenue) as revenue from {{ ref('gold_revenue_by_state') }}
),

facts_scope as (
    select round(sum(item_revenue), 2) as revenue
    from {{ ref('silver_order_facts') }}
    where order_status in (
        {%- for s in var('revenue_order_statuses') %}'{{ s }}'{% if not loop.last %}, {% endif %}{% endfor -%}
    )
      -- cùng phạm vi với gold: chỉ ngày mô phỏng đã đóng
      and {{ closed_sim_days() }}
)

select
    'by_state_vs_facts' as check_name,
    s.revenue           as gold_revenue,
    f.revenue           as facts_revenue,
    abs(s.revenue - f.revenue) as diff
from by_state s
cross join facts_scope f
-- Ngưỡng 10 thay vì 0: gold làm tròn revenue theo từng (bang, tháng), nên
-- tổng các số đã làm tròn lệch nhẹ so với làm tròn của tổng (khoảng 700 nhóm
-- x 0,005 -> sai số tích luỹ tối đa ~3,5).
where abs(s.revenue - f.revenue) > 10.0
