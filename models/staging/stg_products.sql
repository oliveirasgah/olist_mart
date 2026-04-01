SELECT
  p.product_id,
  COALESCE(ct.product_category_name_english, 'unknown') AS product_category_name,
  p.product_category_name AS product_category_name_pt,
  p.product_name_length,
  p.product_description_lenght AS product_description_length,
  p.product_photos_qty,
  p.product_weight_g,
  p.product_length_cm,
  p.product_height_cm,
  p.product_width_cm
FROM
  {{ source('raw', 'products') }} p
  LEFT JOIN {{ source('raw', 'category_translation') }} ct ON ct.product_category_name = p.product_category_name;
