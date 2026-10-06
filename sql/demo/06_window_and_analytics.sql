-- ===========================================================================
-- 06. SQL PHÂN TÍCH: WINDOW FUNCTION, GROUPING SETS, ARRAY/MAP, JSON
--
-- Trino hỗ trợ ANSI SQL đầy đủ cho phân tích: window function, grouping sets,
-- kiểu phức hợp, hàm xấp xỉ, JSON, MATCH_RECOGNIZE.
-- ===========================================================================

-- --- 6.1 Window function: xếp hạng và so với kỳ trước --------------------
WITH monthly AS (
    SELECT
        c.customer_state,
        date_trunc('month', o.order_purchase_timestamp) AS order_month,
        round(sum(i.price + i.freight_value), 2)        AS revenue
    FROM lakehouse.bronze.orders      o
    JOIN lakehouse.bronze.order_items i ON o.order_id = i.order_id
    JOIN lakehouse.bronze.customers   c ON o.customer_id = c.customer_id
    WHERE o.order_status = 'delivered'
    GROUP BY c.customer_state, date_trunc('month', o.order_purchase_timestamp)
)
SELECT
    customer_state,
    order_month,
    revenue,

    -- kỳ trước và tăng trưởng
    lag(revenue) OVER (PARTITION BY customer_state ORDER BY order_month) AS prev_month,
    round(
        100.0 * (revenue - lag(revenue) OVER (PARTITION BY customer_state ORDER BY order_month))
        / nullif(lag(revenue) OVER (PARTITION BY customer_state ORDER BY order_month), 0)
    , 1) AS growth_pct,

    -- trung bình trượt 3 tháng
    round(avg(revenue) OVER (
        PARTITION BY customer_state ORDER BY order_month
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ), 2) AS ma3,

    -- luỹ kế từ đầu
    round(sum(revenue) OVER (
        PARTITION BY customer_state ORDER BY order_month
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ), 2) AS running_total,

    -- xếp hạng bang trong tháng
    rank()       OVER (PARTITION BY order_month ORDER BY revenue DESC) AS rank_in_month,
    dense_rank() OVER (PARTITION BY order_month ORDER BY revenue DESC) AS dense_rank_in_month,
    ntile(4)     OVER (PARTITION BY order_month ORDER BY revenue DESC) AS quartile,
    round(percent_rank() OVER (PARTITION BY order_month ORDER BY revenue), 3) AS pct_rank

FROM monthly
WHERE customer_state IN ('SP', 'RJ', 'MG', 'RS', 'PR')
ORDER BY customer_state, order_month;


-- --- 6.2 GROUPING SETS / ROLLUP / CUBE: nhiều mức tổng trong 1 lần quét --
SELECT
    COALESCE(c.customer_state, '(tất cả)')     AS state,
    COALESCE(o.order_status,   '(tất cả)')     AS status,
    count(*)                                   AS orders,
    grouping(c.customer_state, o.order_status) AS grouping_level
FROM lakehouse.bronze.orders   o
JOIN lakehouse.bronze.customers c ON o.customer_id = c.customer_id
GROUP BY ROLLUP (c.customer_state, o.order_status)
ORDER BY grouping_level, state, status;


-- --- 6.3 Array / Map: gom danh sách sản phẩm của mỗi đơn -----------------
SELECT
    o.order_id,
    array_agg(i.product_id ORDER BY i.order_item_id)            AS products,
    cardinality(array_agg(i.product_id))                        AS n_items,
    map_agg(i.order_item_id, i.price)                           AS price_by_item,
    array_max(array_agg(i.price))                               AS max_price,
    reduce(array_agg(i.price), CAST(0.0 AS double),
           (s, x) -> s + x, s -> s)                            AS total_price
FROM lakehouse.bronze.orders      o
JOIN lakehouse.bronze.order_items i ON o.order_id = i.order_id
GROUP BY o.order_id
HAVING count(*) >= 3
LIMIT 20;

-- Bung array ngược lại thành dòng
SELECT order_id, product_id
FROM (
    SELECT o.order_id, array_agg(i.product_id) AS products
    FROM lakehouse.bronze.orders      o
    JOIN lakehouse.bronze.order_items i ON o.order_id = i.order_id
    GROUP BY o.order_id
    LIMIT 5
)
CROSS JOIN UNNEST(products) AS t(product_id);


-- --- 6.4 Hàm xấp xỉ: nhanh hơn nhiều khi chỉ cần "gần đúng" --------------
SELECT
    count(DISTINCT customer_id)             AS exact_customers,
    approx_distinct(customer_id)            AS approx_customers,
    approx_percentile(
        CAST(date_diff('day', order_purchase_timestamp,
                       order_delivered_customer_date) AS double),
        ARRAY[0.5, 0.9, 0.99]
    )                                       AS delivery_days_p50_p90_p99
FROM lakehouse.bronze.orders
WHERE order_delivered_customer_date IS NOT NULL;


-- --- 6.5 Xử lý chuỗi và JSON ---------------------------------------------
SELECT
    review_score,
    count(*)                                            AS n,
    round(avg(length(review_comment_message)), 1)       AS avg_comment_len,
    -- dựng JSON ngay trong SQL.
    -- json_object(... : ...) trả về varchar, không bọc thêm json_format()
    -- (json_format chỉ nhận kiểu json).
    json_object(
        'score'    : review_score,
        'reviews'  : count(*),
        'sample'   : substr(max(review_comment_title), 1, 40)
    )                                                   AS as_json
FROM lakehouse.bronze.reviews
WHERE review_comment_message IS NOT NULL
GROUP BY review_score
ORDER BY review_score;


-- --- 6.6 Pattern matching trên chuỗi dòng (MATCH_RECOGNIZE) --------------
-- Tìm các bang có 3 tháng tăng trưởng doanh thu liên tiếp.
WITH monthly AS (
    SELECT
        c.customer_state,
        date_trunc('month', o.order_purchase_timestamp) AS order_month,
        sum(i.price) AS revenue
    FROM lakehouse.bronze.orders      o
    JOIN lakehouse.bronze.order_items i ON o.order_id = i.order_id
    JOIN lakehouse.bronze.customers   c ON o.customer_id = c.customer_id
    GROUP BY c.customer_state, date_trunc('month', o.order_purchase_timestamp)
)
SELECT *
FROM monthly
MATCH_RECOGNIZE (
    PARTITION BY customer_state
    ORDER BY order_month
    MEASURES
        FIRST(up.order_month) AS start_month,
        LAST(up.order_month)  AS end_month,
        count(up.*)           AS months_up
    ONE ROW PER MATCH
    PATTERN (strt up{3,})
    DEFINE up AS up.revenue > PREV(up.revenue)
)
ORDER BY months_up DESC, customer_state;
