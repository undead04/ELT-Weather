{{ 
  config(
    materialized = 'table', 
    tags = ['static_dim'],
    post_hook=[
      "{{ log_row_count(this) }}",
      "{{ log_execution_time(this) }}"
    ]
  ) 
}}
SELECT to_char(datum, 'YYYYMMDD')::INT AS date_key,
    datum AS full_date,
    extract(
        year
        FROM datum
    ) AS year,
    extract(
        month
        FROM datum
    ) AS month,
    to_char(datum, 'TMMonth') AS month_name,
    extract(
        day
        FROM datum
    ) AS day,
    extract(
        dow
        FROM datum
    ) AS day_of_week,
    extract(quarter FROM datum) AS quarter,
    extract(dow FROM datum) = 6 AS is_weekend
FROM generate_series(
        '2024-01-01'::DATE,
        '2030-12-31'::DATE,
        '1 day'::interval
    ) datum