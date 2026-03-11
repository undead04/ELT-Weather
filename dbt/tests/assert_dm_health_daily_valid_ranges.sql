-- Test: Health metrics should be within expected ranges for marts
-- Fails if aggregated metrics are outside reasonable bounds
-- Severity: ERROR

WITH health_metrics AS (
    SELECT
        date_key,
        province_id,
        avg_temp_c,
        avg_pm2_5,
        max_uv_index
    FROM {{ ref('dm_health_daily') }}
)

SELECT 
    date_key,
    province_id,
    avg_temp_c,
    avg_pm2_5,
    max_uv_index,
    CASE 
        WHEN avg_temp_c < -50 OR avg_temp_c > 60 THEN 'Invalid temperature'
        WHEN avg_pm2_5 < 0 OR avg_pm2_5 > 500 THEN 'Invalid PM2.5'
        WHEN max_uv_index < 0 OR max_uv_index > 15 THEN 'Invalid UV index'
    END as error_message
FROM health_metrics
WHERE 
    avg_temp_c < -50 OR avg_temp_c > 60
    OR avg_pm2_5 < 0 OR avg_pm2_5 > 500
    OR max_uv_index < 0 OR max_uv_index > 15
