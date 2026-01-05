INSERT INTO dim_time (hour, minute, second, time_bucket,inseget_time)
SELECT 
    hour,
    minute,
    second,
    time_bucket,
    inseget_time
FROM stg_dim_time
ON CONFLICT (hour, minute, second)
DO UPDATE SET
    hour = EXCLUDED.hour,
    minute = EXCLUDED.minute,
    second = EXCLUDED.second,
    time_bucket = EXCLUDED.time_bucket,
    inseget_time = EXCLUDED.inseget_time;
