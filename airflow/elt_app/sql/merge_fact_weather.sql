INSERT INTO fact_weather (date_id, city_id, time_id, 
temperature, humidity, wind_speed, precipitation, 
weather_code, cloud_cover, rain, wind_direction, weather_type, apparent_temperature,
inseget_time)
SELECT
    d.date_id,
    c.city_id,
    t.time_id,
    temperature,
    humidity,
    wind_speed,
    precipitation,
    weather_code,
    cloud_cover,
    rain,
    wind_direction,
    weather_type,
    apparent_temperature,
    i.inseget_time
FROM stg_fact_weather i JOIN 
    dim_date d on d.full_date = i.date
    JOIN dim_time t on t.hour = i.hour
    JOIN dim_city c on c.city_name = i.city_name    
ON CONFLICT (date_id, city_id, time_id) 
DO UPDATE SET
    temperature = EXCLUDED.temperature,
    humidity = EXCLUDED.humidity,
    wind_speed = EXCLUDED.wind_speed,
    precipitation = EXCLUDED.precipitation,
    weather_code = EXCLUDED.weather_code,
    cloud_cover = EXCLUDED.cloud_cover,
    rain = EXCLUDED.rain,
    wind_direction = EXCLUDED.wind_direction,
    weather_type = EXCLUDED.weather_type,
    apparent_temperature = EXCLUDED.apparent_temperature,
    inseget_time = EXCLUDED.inseget_time;