INSERT INTO fact_air_quality (date_id, city_id, time_id, aqi, pm25, pm10, no2, so2, o3, co, co2,inseget_time)
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
    co2,
    i.inseget_time
FROM stg_fact_air_quality i JOIN 
    dim_date d on d.full_date = i.date
    JOIN dim_time t on t.hour = i.hour
    JOIN dim_city c on c.city_name = i.city_name    
ON CONFLICT (date_id, city_id, time_id) 
DO UPDATE SET
    aqi = EXCLUDED.aqi,
    pm25 = EXCLUDED.pm25,
    pm10 = EXCLUDED.pm10,
    no2 = EXCLUDED.no2,
    so2 = EXCLUDED.so2,
    o3 = EXCLUDED.o3,
    co = EXCLUDED.co,
    co2 = EXCLUDED.co2,
    inseget_time = EXCLUDED.inseget_time;
