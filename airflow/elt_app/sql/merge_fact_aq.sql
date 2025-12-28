INSERT INTO fact_air_quality (date_id, city_id, time_id, aqi, pm25, pm10, no2, so2, o3, co, co2)
SELECT
    d.date_id,
    c.city_id,
    t.time_id,
    aqi,
    pm25,
    pm10,
    no2,
    so2,
    o3,
    co,
    co2
FROM stg_fact_air_quality i JOIN 
    dim_date d on d.full_date = i.date
    JOIN dim_time t on t.hour = i.hour
    JOIN dim_city c on c.city_name = i.city_name    
ON CONFLICT (date_id, city_id, time_id) DO NOTHING