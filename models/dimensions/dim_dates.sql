WITH
  date_spine AS (
    SELECT
      CAST(
        UNNEST(
          GENERATE_SERIES(
            CAST('2016-01-01' AS DATE),
            CAST('2018-12-31' AS DATE),
            INTERVAL '1 day'
          )
        ) AS DATE
      ) AS date_day
  )
SELECT
  date_day,
  YEAR (date_day) AS YEAR,
  MONTH (date_day) AS MONTH,
  DAY (date_day) AS DAY,
  QUARTER (date_day) AS quarter,
  WEEKOFYEAR (date_day) AS week_of_year,
  DAYOFWEEK (date_day) AS day_of_week,
  DAYNAME (date_day) AS day_name,
  MONTHNAME (date_day) AS month_name,
  DATE_TRUNC('month', date_day) AS first_day_of_month,
  CAST(
    DATE_TRUNC('month', date_day) + INTERVAL '1 month' - INTERVAL '1 day' AS DATE
  ) AS last_day_of_month,
  CASE
    WHEN DAYOFWEEK (date_day) IN (1, 7) THEN TRUE
    ELSE FALSE
  END AS is_weekend
FROM
  date_spine
