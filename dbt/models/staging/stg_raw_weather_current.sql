{{ config(
    materialized='incremental',
    unique_key=['province_id','event_time'],
    tags=['current_flow'],
    pre_hook=[
      "{{ log_source_freshness(source('raw', 'raw_weather_current')) }}",
      "{{ log_raw_record_count('raw', 'raw_weather_current', 'time::timestamp') }}"
    ],
    post_hook=[
      "{{ log_row_count(this) }}",
      "{{ log_data_quality(['temperature_2m', 'relative_humidity_2m', 'apparent_temperature', 'uv_index', 'precipitation', 'wind_speed']) }}",
      "{{ log_execution_time(this) }}"  
    ]
) }}
SELECT
    province_id,
    time::timestamp AS event_time,
    temperature_2m::float AS temperature_2m,
    relative_humidity_2m::float AS relative_humidity_2m,
    apparent_temperature::float AS apparent_temperature,
    uv_index::float AS uv_index,
    precipitation::float AS precipitation,
    wind_speed::float AS wind_speed,
    insert_time
FROM {{ source('raw', 'raw_weather_current') }}
WHERE {{ get_date_filter('time::timestamp') }}
{% if is_incremental() and not var('is_backfill', false) %}
  AND insert_time > (select coalesce(max(insert_time), '1900-01-01') from {{ this }})
{% endif %}
