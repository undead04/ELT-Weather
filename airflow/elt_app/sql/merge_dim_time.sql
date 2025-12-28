INSERT INTO dim_time (hour, minute, second, time_bucket)
SELECT 
    hour,
    minute,
    second,
    time_bucket
FROM stg_dim_time
ON CONFLICT (hour, minute, second) DO NOTHING;
