{{ config(
    materialized='incremental',
    unique_key=['province_id','date_key','time_key'],
    tags = ['forecast_flow'],
    post_hook=[
      "{{ log_row_count(this) }}",
      "{{ log_data_quality(['temperature_2m', 'relative_humidity_2m', 'apparent_temperature', 'uv_index', 'precipitation', 'wind_speed']) }}",
      "{{ log_execution_time(this) }}"  
    ]
) }}

WITH weather AS (
    SELECT * FROM {{ ref('stg_raw_weather_forecast') }}
    WHERE {{ get_date_filter('event_time') }}
    {% if is_incremental() and not var('is_backfill', false) %}
      AND insert_time > (select coalesce(max(insert_time), '1900-01-01') from {{ this }})
    {% endif %}
),

aq AS (
    SELECT * FROM {{ ref('stg_raw_aq_forecast') }}
    WHERE {{ get_date_filter('event_time') }}
    {% if is_incremental() and not var('is_backfill', false) %}
      AND insert_time > (select coalesce(max(insert_time), '1900-01-01') from {{ this }})
    {% endif %}
),

dim_time AS (
    SELECT * FROM {{ ref('dim_time') }}
),

dim_date AS (
    SELECT * FROM {{ ref('dim_date') }}
)

SELECT
    w.province_id,
    t.time_key,
    d.date_key,
    w.event_time,
    w.temperature_2m,
    w.relative_humidity_2m,
    w.apparent_temperature,
    w.uv_index,
    w.precipitation,
    w.wind_speed,
    a.pm2_5,
    a.european_aqi_pm2_5,

    CASE
        WHEN w.temperature_2m > 27 THEN                
            -8.784695 + 1.61139411 * w.temperature_2m + 2.338549 * w.relative_humidity_2m 
            - 0.14611605 * w.temperature_2m * w.relative_humidity_2m 
            - 0.01230809 * POWER(w.temperature_2m, 2) 
            - 0.01642482 * POWER(w.relative_humidity_2m, 2) 
            + 0.00221173 * POWER(w.temperature_2m, 2) * w.relative_humidity_2m 
            + 0.00072546 * w.temperature_2m * POWER(w.relative_humidity_2m, 2) 
            - 0.00000358 * POWER(w.temperature_2m, 2) * POWER(w.relative_humidity_2m, 2)
        ELSE NULL                
    END AS heat_index,

    CASE
        WHEN w.temperature_2m < 10 THEN                
            13.12 + (0.6215 * w.temperature_2m) 
            - (11.37 * POWER(w.wind_speed, 0.16)) 
            + (0.3965 * w.temperature_2m * POWER(w.wind_speed, 0.16))
        ELSE NULL
    END AS wind_chill,
    w.insert_time,
    current_timestamp AS update_at

FROM weather w
INNER JOIN aq a
    ON w.province_id = a.province_id
   AND w.event_time = a.event_time
INNER JOIN dim_time t
    ON extract(hour from w.event_time) = t.hour_24h_int
   AND extract(minute from w.event_time) = t.minute_int
INNER JOIN dim_date d
    ON CAST(w.event_time AS DATE) = d.full_date
