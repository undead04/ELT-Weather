INSERT INTO dim_date (full_date, day, month, year, day_of_week, quarter, is_weekend, inseget_time)
SELECT 
    full_date,
    day,
    month,
    year,
    day_of_week,
    quarter,
    is_weekend,
    inseget_time
FROM stg_dim_date
ON CONFLICT (day, month, year)
DO UPDATE SET
    full_date = EXCLUDED.full_date,
    day = EXCLUDED.day,
    month = EXCLUDED.month,
    year = EXCLUDED.year,
    day_of_week = EXCLUDED.day_of_week,
    quarter = EXCLUDED.quarter,
    is_weekend = EXCLUDED.is_weekend,
    inseget_time = EXCLUDED.inseget_time;

