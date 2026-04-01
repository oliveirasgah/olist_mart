SELECT
  order_id,
  payment_sequential,
  payment_type,
  payment_installments AS installments,
  CAST(payment_value AS DECIMAL(10, 2)) AS payment_value
FROM
  {{ source('raw', 'payments') }};
