-- Test: Data freshness - Raw data should not be too old
-- Warns if latest raw data is older than 1 hour
-- Severity: WARN (không fail pipeline, chỉ cảnh báo)

WITH latest_inserts AS (
    SELECT 
        'raw_weather_current' as table_name,
        MAX(insert_time) as latest_insert,
        EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - MAX(insert_time)))/60 as age_minutes
    FROM {{ source('raw', 'raw_weather_current') }}
    
    UNION ALL
    
    SELECT 
        'raw_aq_current' as table_name,
        MAX(insert_time) as latest_insert,
        EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - MAX(insert_time)))/60 as age_minutes
    FROM {{ source('raw', 'raw_aq_current') }}
)

SELECT 
    table_name,
    latest_insert,
    ROUND(age_minutes::numeric, 1) as age_minutes,
    'Data is stale! Last insert > 60 minutes ago' as warning_message
FROM latest_inserts
WHERE age_minutes > 60
