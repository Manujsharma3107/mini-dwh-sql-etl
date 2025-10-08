-- Quick checks and KPIs
SELECT * FROM v_monthly_kpis;
SELECT * FROM v_top_products;
SELECT * FROM v_category_contribution;
SELECT COUNT(*) AS dq_null_issues FROM dq_nulls;
SELECT COUNT(*) AS dq_negative_qty FROM dq_negative_qty;

-- 1) fact rows must equal raw order_items rows
SELECT
  (SELECT COUNT(*) FROM fact_sales)      AS fact_rows,
  (SELECT COUNT(*) FROM stg_order_items) AS raw_item_rows;

-- 2) every fact row must have a matching order header
SELECT COUNT(*) AS missing_orders
FROM fact_sales fs
LEFT JOIN stg_orders o USING(order_id)
WHERE o.order_id IS NULL;

-- 3) revenue recomputes correctly from raw columns
SELECT COUNT(*) AS mismatched_revenue
FROM fact_sales
WHERE ABS(revenue - (quantity*unit_price*(1-COALESCE(discount,0)))) > 0.001;

-- 4) (nice add) new vs repeat orders per month
WITH first_order AS (
  SELECT customer_id, MIN(order_date) AS first_date
  FROM fact_sales WHERE status IN ('completed','shipped')
  GROUP BY customer_id
)
SELECT substr(fs.order_date,1,7) AS month,
       SUM(CASE WHEN fs.order_date = fo.first_date THEN 1 ELSE 0 END) AS new_orders,
       SUM(CASE WHEN fs.order_date <> fo.first_date THEN 1 ELSE 0 END) AS repeat_orders
FROM fact_sales fs
JOIN first_order fo USING(customer_id)
GROUP BY 1
ORDER BY 1;

-- 5) (nice add) simple refund/cancel rate by month
SELECT substr(order_date,1,7) AS month,
       ROUND(100.0 * SUM(CASE WHEN status='cancelled' THEN 1 ELSE 0 END)
             / NULLIF(COUNT(*),0), 2) AS cancel_rate_pct
FROM fact_sales
GROUP BY 1
ORDER BY 1;
