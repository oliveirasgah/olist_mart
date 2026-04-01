WITH
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
      {{ ref('stg_sellers') }}
  )
SELECT DISTINCT
  oi.seller_id
FROM
  order_items oi
  LEFT JOIN sellers s ON s.seller_id = oi.seller_id
WHERE
  s.seller_id IS NULL
