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
  sellers AS (
    SELECT
      *
    FROM
      {{ ref('dim_sellers') }}
  )
SELECT
  s.seller_id,
  s.city,
  s.state,
  COUNT(DISTINCT o.order_id) AS orders_count,
  COUNT(oi.order_item_id) AS items_sold,
  CAST(SUM(oi.price) AS DECIMAL(10, 2)) AS total_revenue,
  CAST(SUM(oi.freight_value) AS DECIMAL(10, 2)) AS total_freight,
  CAST(AVG(oi.price) AS DECIMAL(10, 2)) AS avg_item_price,
  CAST(AVG(o.delivery_time_days) AS DECIMAL(10, 2)) AS avg_delivery_time_days,
  CAST(AVG(o.delivery_delay_days) AS DECIMAL(10, 2)) AS avg_delivery_delay_days,
  SUM(
    CASE
      WHEN o.delivery_delay_days > 0 THEN 1
      ELSE 0
    END
  ) AS late_deliveries_count,
  CAST(
    SUM(
      CASE
        WHEN o.delivery_delay_days > 0 THEN 1
        ELSE 0
      END
    ) * 100.0 / COUNT(DISTINCT o.order_id) AS DECIMAL(5, 2)
  ) AS late_delivery_rate_pct
FROM
  orders o
  INNER JOIN order_items oi ON o.order_id = oi.order_id
  INNER JOIN sellers s ON s.seller_id = oi.seller_id
GROUP BY
  s.seller_id,
  s.city,
  s.state
