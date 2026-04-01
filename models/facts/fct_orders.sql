{{
  config(
    materialized='incremental',
    unique_key='order_id',
    on_schema_change='append_new_columns'
  )
}}

WITH
  orders AS (
    SELECT
      *
    FROM
      {{ ref('stg_orders') }}
    {% if is_incremental() %}
      WHERE purchased_at > (SELECT MAX(purchased_at) FROM {{ this }})
    {% endif %}
  ),
  items AS (
    SELECT
      order_id,
      COUNT(order_item_id) AS items_count,
      SUM(price) AS total_items_price,
      SUM(freight_value) AS total_freight_value,
      SUM(item_total) AS total_order_value
    FROM
      {{ ref('stg_order_items') }}
    GROUP BY
      order_id
  ),
  payments AS (
    SELECT
      order_id,
      SUM(payment_value) AS total_payment_value,
      COUNT(DISTINCT payment_type) AS payment_methods_count,
      MAX(installments) AS max_installments
    FROM
      {{ ref('stg_payments') }}
    GROUP BY
      order_id
  )
SELECT
  o.order_id,
  o.customer_id,
  o.order_status,
  o.purchased_at,
  o.approved_at,
  o.delivered_at,
  o.estimated_delivery_at,
  CAST(o.purchased_at AS DATE) AS purchased_date,
  i.items_count,
  i.total_items_price,
  i.total_freight_value,
  i.total_order_value,
  p.total_payment_value,
  p.payment_methods_count,
  p.max_installments,
  CASE
    WHEN o.delivered_at IS NOT NULL
    AND o.estimated_delivery_at IS NOT NULL THEN DATEDIFF ('day', o.estimated_delivery_at, o.delivered_at)
  END AS delivery_delay_days,
  CASE
    WHEN o.delivered_at IS NOT NULL
    AND o.purchased_at IS NOT NULL THEN DATEDIFF ('day', o.purchased_at, o.delivered_at)
  END AS delivery_time_days
FROM
  orders o
  LEFT JOIN items i ON i.order_id = o.order_id
  LEFT JOIN payments p ON p.order_id = o.order_id
