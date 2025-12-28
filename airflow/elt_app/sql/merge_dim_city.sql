INSERT INTO DIM_CITY (city_name, country, lon, lat)
SELECT 
    city_name,
    country,
    lon,
    lat
FROM stg_dim_city
ON CONFLICT (city_name) DO NOTHING;
