{{
  config(
    materialized='incremental',
    tags=['forecast_flow'],
    unique_key=['date_key', 'time_key', 'province_id'],
    post_hook=[
            "{{ log_row_count(ref('int_weather_aq_forecast')) }}",
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

WITH source_data AS (
    SELECT 
        d.date_key,
        w.event_time,
        t.time_key,
        w.province_id,
        p.province_name,
        w.pm2_5,
        w.uv_index,
        w.precipitation,
        w.apparent_temperature,
        w.update_at
    FROM {{ ref('int_weather_aq_forecast') }} w
    JOIN {{ ref('dim_locations') }} p ON w.province_id = p.province_id
    JOIN {{ ref('dim_date') }} d ON w.date_key = d.date_key
    JOIN {{ ref('dim_time') }} t ON w.time_key = t.time_key
    
    WHERE 1=1
    AND {{ get_date_filter('w.event_time') }}
    
    {% if is_incremental() and not var('is_backfill', false) %}
      -- Bây giờ biến max_val đã là một hằng số, không còn dính dáng đến bảng đích khi compile nữa
      AND w.update_at > '{{ max_val }}'::timestamp
    {% endif %}
),

ScoreCalc AS (
    SELECT 
        date_key,
        time_key,
        province_id,
        province_name,
        event_time,
        -- Chống chia cho 0 hoặc giá trị null
        ((1 - (COALESCE(pm2_5, 0) / 300.0)) * 0.5 + (1 - (COALESCE(uv_index, 0) / 15.0)) * 0.5) * 100 as base_score,
        precipitation,
        apparent_temperature
    FROM source_data
),

FinalScore AS (
    SELECT 
        *,
        CASE
            WHEN precipitation > 5 THEN 0 
            WHEN apparent_temperature > 45 OR apparent_temperature < 0 THEN 10
            ELSE base_score
        END as suitability_score
    FROM ScoreCalc
)

SELECT 
    date_key,
    time_key,
    province_id,
    event_time,
    province_name,  
    suitability_score,
    CASE 
        WHEN suitability_score >= 80 THEN 'Thời tiết tuyệt vời, thoải mái hoạt động!'
        WHEN suitability_score >= 50 THEN 'Nên hạn chế hoạt động mạnh.'
        ELSE 'Độc hại, không ra ngoài!'
    END as advice_text,
    'Open-Meteo' as data_source,
    CURRENT_TIMESTAMP as update_at -- Đây chính là cột sẽ được MAX() ở lần chạy sau
FROM FinalScore