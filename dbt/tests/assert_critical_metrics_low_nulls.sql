-- Test: Critical weather metrics should have low null percentage
-- Fails if null percentage exceeds threshold
-- Severity: ERROR

WITH null_stats AS (
    SELECT
        COUNT(*) as total_rows,
        SUM(CASE WHEN temperature_2m IS NULL THEN 1 ELSE 0 END) as temp_nulls,
        SUM(CASE WHEN pm2_5 IS NULL THEN 1 ELSE 0 END) as pm25_nulls,
        SUM(CASE WHEN uv_index IS NULL THEN 1 ELSE 0 END) as uv_nulls
    FROM {{ ref('int_weather_aq_current') }}
),
null_percentages AS (
    SELECT
        'temperature_2m' as metric,
        temp_nulls as null_count,
        ROUND(100.0 * temp_nulls / NULLIF(total_rows, 0), 2) as null_pct,
        5.0 as max_allowed_pct
    FROM null_stats
    WHERE total_rows > 0
    
    UNION ALL
    
    SELECT
        'pm2_5' as metric,
        pm25_nulls as null_count,
        ROUND(100.0 * pm25_nulls / NULLIF(total_rows, 0), 2) as null_pct,
        10.0 as max_allowed_pct
    FROM null_stats
    WHERE total_rows > 0
    
    UNION ALL
    
    SELECT
        'uv_index' as metric,
        uv_nulls as null_count,
        ROUND(100.0 * uv_nulls / NULLIF(total_rows, 0), 2) as null_pct,
        5.0 as max_allowed_pct
    FROM null_stats
    WHERE total_rows > 0
)

SELECT 
    metric,
    null_count,
    null_pct,
    max_allowed_pct,
    CONCAT(metric, ' has ', null_pct, '% nulls (max allowed: ', max_allowed_pct, '%)') as error_message
FROM null_percentages
WHERE null_pct > max_allowed_pct
