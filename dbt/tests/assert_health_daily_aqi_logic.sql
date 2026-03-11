-- Test AQI logic in dm_health_daily
-- WHO/Standard ranges for PM2.5:
-- 0 - 12: Good
-- 12.1 - 35.4: Moderate
-- 35.5 - 55.4: Unhealthy
-- > 55.4: Hazardous

SELECT *
FROM {{ ref('dm_health_daily') }}
WHERE 
    (avg_pm2_5 <= 12 AND aqi_category != 'Good')
    OR (avg_pm2_5 > 12 AND avg_pm2_5 <= 35.4 AND aqi_category != 'Moderate')
    OR (avg_pm2_5 > 35.4 AND avg_pm2_5 <= 55.4 AND aqi_category != 'Unhealthy')
    OR (avg_pm2_5 > 55.4 AND aqi_category != 'Hazardous')
