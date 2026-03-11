{{ config(
    materialized='incremental',
    unique_key=['province_id','event_time'],
    tags=['forecast_flow'],
    pre_hook=[
      "{{ log_source_freshness(source('raw', 'raw_weather_forecast')) }}",
      "{{ log_raw_record_count('raw', 'raw_weather_forecast', 'insert_time') }}"
    ],
    post_hook=[
      "{{ log_row_count(this) }}",
      "{{ log_data_quality(['temperature_2m', 'relative_humidity_2m', 'apparent_temperature', 'uv_index', 'precipitation', 'wind_speed']) }}",
      "{{ log_execution_time(this) }}"  
    ]
) }}
WITH unnested AS (
    SELECT
        r.province_id,
        t.time::timestamp AS event_time,
        t.temperature_2m::float AS temperature_2m,
        t.relative_humidity_2m::float AS relative_humidity_2m,
        t.apparent_temperature::float AS apparent_temperature,
        t.uv_index::float AS uv_index,
        t.precipitation::float AS precipitation,
        t.wind_speed::float AS wind_speed,
        r.insert_time
    FROM {{ source('raw', 'raw_weather_forecast') }} r
    CROSS JOIN LATERAL UNNEST(
        r.time::timestamp[],                
        r.temperature_2m::float[],
        r.relative_humidity_2m::float[],
        r.apparent_temperature::float[],
        r.uv_index::float[],
        r.precipitation::float[],
        r.wind_speed::float[]
    ) AS t(time, temperature_2m, relative_humidity_2m, apparent_temperature, uv_index, precipitation, wind_speed)
    WHERE {{ get_date_filter('t.time::timestamp') }}
    {% if is_incremental() and not var('is_backfill', false) %}
      AND r.insert_time > (select coalesce(max(insert_time), '1900-01-01') from {{ this }})
    {% endif %}
)
SELECT * FROM unnested
