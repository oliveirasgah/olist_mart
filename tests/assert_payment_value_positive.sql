{{ config(severity='warn') }}

SELECT
  order_id,
  payment_value
FROM
  {{ ref('stg_payments') }}
WHERE
  payment_value <= 0
