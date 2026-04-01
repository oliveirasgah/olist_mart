SELECT
  order_id,
  purchased_at,
  approved_at,
  delivered_at
FROM
  {{ ref('stg_orders') }}
WHERE
  order_status = 'delivered'
  AND (
    purchased_at > approved_at
    OR purchased_at > delivered_at
    OR delivered_at > approved_at
  )
