{{
  config(
    tags=['historical_flow'],
    materialized='incremental',
    unique_key=['province_id', 'date_key'],
    indexes=[
      {'columns': ['province_id', 'date_key'], 'unique': True},
      {'columns': ['date_key']}
    ],
    post_hook=[
      "{{ log_row_count(ref('int_weather_aq_current')) }}",
      "{{ log_aggregation_summary('province_id', 'avg_temp_c') }}",
      "{{ log_aggregation_summary('province_id', 'avg_pm2_5') }}",
      "{{ log_execution_time(this) }}"
    ]
  )
}}
-- CHỈ tạo CTE này khi đang ở chế độ incremental
{% if is_incremental() and not var('is_backfill', false) %}
    {% set last_update_query %}
        SELECT COALESCE(MAX(update_at), '1900-01-01') FROM {{ this }}
    {% endset %}
    {% set max_val = run_query(last_update_query).columns[0][0] %}
{% else %}
    {% set max_val = '1900-01-01' %}
{% endif %}

WITH DailyAgg AS (
    SELECT 
        w.date_key,
        p.province_id,
        p.province_name,
        d.full_date,
        MAX(w.uv_index) as max_uv_index,
        AVG(w.pm2_5) as avg_pm2_5,
        COALESCE(MAX(w.heat_index), 0) as heat_index_max,
        AVG(w.precipitation) as avg_precipitation,
        COALESCE(MIN(w.wind_chill), 0) as wind_chill_min,
        AVG(w.temperature_2m) as avg_temp_c,
        AVG(w.relative_humidity_2m) as avg_humidity
    FROM {{ ref('int_weather_aq_current') }} w
    JOIN {{ ref('dim_locations') }} p 
        ON w.province_id = p.province_id
    JOIN {{ ref('dim_date') }} d 
        ON w.date_key = d.date_key     
    WHERE 1=1
    AND {{ get_date_filter('w.event_time') }}
    {% if is_incremental() and not var('is_backfill', false) %}
      AND w.update_at > '{{ max_val }}'::timestamp
    {% endif %}
    GROUP BY 
        w.date_key, 
        p.province_id, 
        p.province_name, 
        d.full_date
)
SELECT 
    date_key,
    province_id,
    province_name,
    full_date,
    max_uv_index,
    avg_temp_c,
    avg_humidity,
    avg_pm2_5,
    heat_index_max,
    avg_precipitation,
    wind_chill_min,

    CASE 
        WHEN avg_pm2_5 <= 12 THEN 'Good'
        WHEN avg_pm2_5 <= 35.4 THEN 'Moderate'
        WHEN avg_pm2_5 <= 55.4 THEN 'Unhealthy'
        ELSE 'Hazardous'
    END as aqi_category,

    CASE 
        WHEN heat_index_max >= 41 THEN 'Extreme Heat'
        WHEN avg_precipitation >= 50 THEN 'Extreme Rain'
        WHEN heat_index_max >= 32 THEN 'Heat'
        WHEN wind_chill_min <= -40 THEN 'Extreme Cold'
        WHEN wind_chill_min <= -20 THEN 'Cold'
        WHEN avg_pm2_5 >= 55.5 THEN 'High Pollution'
        WHEN avg_pm2_5 >= 35.5 THEN 'Moderate Pollution'
        WHEN max_uv_index >= 11 THEN 'Extreme UV'
        WHEN max_uv_index >= 8 THEN 'High UV'
        ELSE 'Safe'
    END as main_risk_factor,
    CURRENT_TIMESTAMP as update_at
FROM DailyAgg
