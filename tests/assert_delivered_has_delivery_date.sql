SELECT
  order_id,
  delivered_at
FROM
  {{ ref('stg_orders') }}
WHERE
  order_status = 'delivered'
  AND delivered_at IS NULL
