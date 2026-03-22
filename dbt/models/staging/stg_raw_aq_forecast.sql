{{ config(
    materialized='table',
    tags=['forecast_flow'],
    pre_hook=[
      "{{ log_source_freshness(source('raw', 'raw_aq_forecast')) }}",
      "{{ log_raw_record_count('raw', 'raw_aq_forecast','insert_time') }}",
    ],
    post_hook=[
      "{{ log_execution_time(this) }}"
    ]
) }}

WITH unnested AS (
    SELECT
        r.province_id,         -- Lấy các cột meta từ bảng gốc r
        r.insert_time,
        t.time AS event_time,  -- Alias rõ ràng để tránh trùng với hàm time()
        t.pm2_5,               -- Chỉ lấy pm2_5 từ bảng t (đã unnest)
        t.european_aqi_pm2_5
    FROM {{ source('raw', 'raw_aq_forecast') }} r
    CROSS JOIN LATERAL UNNEST(
        r.time::timestamp[],
        r.pm2_5::float[],
        r.european_aqi_pm2_5::float[]
    ) AS t(time, pm2_5, european_aqi_pm2_5)               
    WHERE {{ get_date_filter('t.time::timestamp') }}
)

SELECT * FROM unnested