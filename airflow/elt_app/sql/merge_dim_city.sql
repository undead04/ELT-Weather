INSERT INTO dim_city (
    city_name,
    country,
    lon,
    lat,
    min_lat,
    max_lat,
    min_lon,
    max_lon,
    inseget_time
)
SELECT 
    city_name,
    country,
    lon,
    lat,
    min_lat,
    max_lat,
    min_lon,
    max_lon,
    inseget_time
FROM stg_dim_city
ON CONFLICT (city_name)
DO UPDATE SET
    country  = EXCLUDED.country,
    lon      = EXCLUDED.lon,
    lat      = EXCLUDED.lat,
    min_lat  = EXCLUDED.min_lat,
    max_lat  = EXCLUDED.max_lat,
    min_lon  = EXCLUDED.min_lon,
    max_lon  = EXCLUDED.max_lon,
    inseget_time = EXCLUDED.inseget_time;
