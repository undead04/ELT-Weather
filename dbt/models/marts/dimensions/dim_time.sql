{{ 
  config(
    materialized = 'incremental', 
    unique_key = 'time_key',
    tags = ['static_dim'],
    post_hook=[
      "{{ log_execution_time(this) }}"
    ]
  ) 
}}

WITH minute_series AS (
    -- Tạo chuỗi từ 0 đến 1439 phút (24 giờ * 60 phút)
    SELECT generate_series(0, 1439) AS total_minutes
)

SELECT
    total_minutes AS time_key,
    
    -- Tính giờ (0-23)
    (total_minutes / 60) AS hour_24h_int,
    
    -- Tính phút (0-59)
    (total_minutes % 60) AS minute_int,
    
    -- Định dạng HH:mm
    lpad((total_minutes / 60)::text, 2, '0') || ':' || lpad((total_minutes % 60)::text, 2, '0') AS time_formatted,
    
    -- Phân loại buổi trong ngày (dựa trên giờ)
    CASE
        WHEN (total_minutes / 60) BETWEEN 5 AND 10 THEN 'Morning'
        WHEN (total_minutes / 60) BETWEEN 11 AND 14 THEN 'Noon'
        WHEN (total_minutes / 60) BETWEEN 15 AND 18 THEN 'Afternoon'
        ELSE 'Night'
    END AS period_name
FROM minute_series