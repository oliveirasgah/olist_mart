WITH
  orders AS (
    SELECT
      *
    FROM
      {{ ref('fct_orders') }}
    WHERE
      order_status = 'delivered'
  ),
  order_items AS (
    SELECT
      *
    FROM
      {{ ref('stg_order_items') }}
  ),
  products AS (
    SELECT
      *
    FROM
      {{ ref('dim_products') }}
  )
SELECT
  p.product_category_name,
  DATE_TRUNC('month', o.purchased_at) AS revenue_month,
  COUNT(DISTINCT o.order_id) AS orders_count,
  SUM(oi.order_item_id) AS items_sold,
  CAST(SUM(oi.price) AS DECIMAL(10, 2)) AS total_revenue,
  CAST(SUM(oi.freight_value) AS DECIMAL(10, 2)) AS total_freight,
  CAST(SUM(oi.item_total) AS DECIMAL(10, 2)) AS total_revenue_with_freight,
  CAST(AVG(oi.price) AS DECIMAL(10, 2)) AS avg_item_price,
  CAST(AVG(o.total_order_value) AS DECIMAL(10, 2)) AS avg_order_value,
  CAST(AVG(o.delivery_delay_days) AS INT) AS avg_delivery_days
FROM
  orders o
  INNER JOIN order_items oi ON o.order_id   = oi.order_id
  INNER JOIN products p ON p.product_id = oi.product_id
GROUP BY
  p.product_category_name,
  DATE_TRUNC('month', o.purchased_at)
