-- Test: All province_ids must exist in dim_locations
-- Fails if any orphaned province_id is found
-- Severity: ERROR

WITH staging_provinces AS (
    SELECT DISTINCT province_id 
    FROM {{ ref('int_weather_aq_current') }}
),
valid_provinces AS (
    SELECT province_id 
    FROM {{ ref('dim_locations') }}
)

SELECT 
    sp.province_id,
    'Orphaned province_id not in dim_locations!' as error_message
FROM staging_provinces sp
LEFT JOIN valid_provinces vp ON sp.province_id = vp.province_id
WHERE vp.province_id IS NULL
