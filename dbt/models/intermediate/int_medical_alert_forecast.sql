{{
  config(
    materialized = 'table',
    unique_key=['province_id', 'date_key', 'time_key'],
    post_hook=[
                "{{ log_row_count(ref('int_weather_aq_forecast')) }}",
        "{{ log_execution_time(this) }}"
    ],
    tags=['forecast_flow'],
  )
}}
SELECT 
    p.province_id,
    d.date_key,
    t.time_key,
    w.event_time,
    p.province_name,
    -- Phân loại rủi ro chi tiết
    CASE 
        WHEN w.pm2_5 >= 35.5 THEN 'Respiratory'
        WHEN w.uv_index >= 8 THEN 'Skin/Eyes'
        WHEN w.heat_index >= 32 OR w.wind_chill <= -10 THEN 'Cardiovascular'                
        WHEN w.precipitation > 5 THEN 'Safety/Accessibility'
        ELSE 'Unknown'
    END as risk_type,
    -- Mức độ rủi ro
    CASE 
        WHEN w.pm2_5 >= 55.5 OR w.uv_index >= 11 OR w.heat_index >= 41 OR w.wind_chill <= -18 OR w.precipitation > 10 THEN 'Extreme'
        WHEN w.pm2_5 >= 35.5 OR w.uv_index >= 8 OR w.heat_index >= 32 OR w.wind_chill <= -10 OR w.precipitation > 5 THEN 'High'                
        ELSE 'Moderate'
    END as risk_level,
    -- Recommendation
    CASE 
        WHEN w.precipitation > 10 THEN 'Mưa rất to, cần hỗ trợ di chuyển đặc biệt.'
        WHEN w.pm2_5 >= 55.5 THEN 'Đeo khẩu trang N95, hạn chế tối đa ra ngoài.'
        WHEN w.uv_index >= 11 THEN 'Tránh ánh nắng trực tiếp, nguy cơ sốc nhiệt.'
        WHEN w.heat_index >= 41 THEN 'Nguy cơ sốc nhiệt cao, ưu tiên xe có điều hòa.'
        WHEN w.wind_chill <= -18 THEN 'Nguy cơ hạ thân nhiệt, cần xe ấm.'                
        ELSE 'Cân nhắc hoãn lịch khám nếu không cần thiết.'
    END as recommendation,
    -- Affected Population
    CASE 
        WHEN w.pm2_5 >= 55.5 OR w.uv_index >= 11 OR w.heat_index >= 41 OR w.wind_chill <= -18 OR w.precipitation > 10 THEN 'Everyone'
        ELSE 'Sensitive Groups'
    END as affected_population,
    'Forecast' as alert_type,
    CURRENT_TIMESTAMP as update_at
FROM 
    {{ ref("int_weather_aq_forecast") }} w
    JOIN {{ ref('dim_locations') }} p ON w.province_id = p.province_id
    JOIN {{ ref('dim_date') }} d ON w.date_key = d.date_key
    JOIN {{ ref('dim_time') }} t ON w.time_key = t.time_key
WHERE 1 = 1 
  AND {{ get_date_filter('event_time') }}
  AND (
    pm2_5 >= 35.5 
    OR uv_index >= 8 
    OR apparent_temperature >= 32 
    OR apparent_temperature <= 0
    OR precipitation > 5
    )
