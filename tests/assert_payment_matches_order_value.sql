WITH
  orders AS (
    SELECT
      order_id,
      total_order_value,
      total_payment_value
    FROM
      {{ ref('fct_orders') }}
  )
SELECT
  order_id,
  total_order_value,
  total_payment_value,
  ABS(total_payment_value - total_order_value) AS difference
FROM
  orders
WHERE
  ABS(total_payment_value - total_order_value) > 0.05
