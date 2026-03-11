{{ config(
    materialized='incremental',
    unique_key=['province_id','event_time'],
    tags=['forecast_flow'],
    pre_hook=[
      "{{ log_source_freshness(source('raw', 'raw_aq_forecast')) }}",
      "{{ log_raw_record_count('raw', 'raw_aq_forecast','insert_time') }}",
      "{{ log_incremental_stats() }}"
    ],
    post_hook=[
      "{{ log_row_count(this) }}",
      "{{ log_data_quality(['pm2_5', 'european_aqi_pm2_5']) }}",
      "{{ log_execution_time(this) }}"
    ]
) }}
WITH unnested AS (
    SELECT
        r.province_id,
        t.time::timestamp AS event_time,
        t.pm2_5::float AS pm2_5,
        t.european_aqi_pm2_5::float AS european_aqi_pm2_5,
        r.insert_time
    FROM {{ source('raw', 'raw_aq_forecast') }} r
    CROSS JOIN LATERAL UNNEST(
        r.time::timestamp[],
        r.pm2_5::float[],
        r.european_aqi_pm2_5::float[]
    ) AS t(time, pm2_5, european_aqi_pm2_5)               
    WHERE {{ get_date_filter('t.time::timestamp') }}
    {% if is_incremental() and not var('is_backfill', false) %}
      AND r.insert_time > (select coalesce(max(insert_time), '1900-01-01') from {{ this }})
    {% endif %}
)
SELECT * FROM unnested
