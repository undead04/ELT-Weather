INSERT INTO fact_weather (date_id, city_id, time_id, temperature, humidity, wind_speed, precipitation, weather_code, cloud_cover, rain, wind_direction, apparent_temperature)
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
    apparent_temperature
FROM stg_fact_weather i JOIN 
    dim_date d on d.full_date = i.date
    JOIN dim_time t on t.hour = i.hour
    JOIN dim_city c on c.city_name = i.city_name    
ON CONFLICT (date_id, city_id, time_id) DO NOTHING