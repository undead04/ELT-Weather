{{ config(
    materialized='incremental',
    unique_key=['province_id','event_time'],
    tags=['current_flow'],
    pre_hook=[
      "{{ log_source_freshness(source('raw', 'raw_aq_current')) }}",
      "{{ log_raw_record_count('raw', 'raw_aq_current', 'time::timestamp') }}"
    ],
    post_hook=[
      "{{ log_row_count(this) }}",
      "{{ log_data_quality(['pm2_5', 'european_aqi_pm2_5']) }}",
      "{{ log_execution_time(this) }}"  
    ]
) }}
SELECT
    province_id,
    time::timestamp AS event_time,
    pm2_5::float AS pm2_5,
    european_aqi_pm2_5::float AS european_aqi_pm2_5,
    insert_time
FROM {{ source('raw', 'raw_aq_current') }}
WHERE {{ get_date_filter('time::timestamp') }}
{% if is_incremental() and not var('is_backfill', false) %}
  AND insert_time > (select coalesce(max(insert_time), '1900-01-01') from {{ this }})
{% endif %}
