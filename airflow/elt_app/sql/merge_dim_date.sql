INSERT INTO dim_date (full_date, day, month, year, day_of_week, quarter, is_weekend)
SELECT 
    full_date,
    day,
    month,
    year,
    day_of_week,
    quarter,
    is_weekend
FROM stg_dim_date
ON CONFLICT (day, month, year) DO NOTHING;
