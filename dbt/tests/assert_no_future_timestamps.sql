-- Test: No records should have timestamps in the future
-- Fails if any record has a future date
-- Severity: ERROR

SELECT 
    province_id,
    event_time,
    current_timestamp as now,
    'Future timestamp detected!' as error_message
FROM {{ ref('int_weather_aq_current') }}
WHERE event_time > current_timestamp
