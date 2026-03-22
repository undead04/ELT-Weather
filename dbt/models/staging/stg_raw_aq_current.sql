{{ config(
    materialized='table',
    tags=['current_flow'],
    pre_hook=[
      "{{ log_source_freshness(source('raw', 'raw_aq_current')) }}",
      "{{ log_raw_record_count('raw', 'raw_aq_current', 'time::timestamp') }}"
    ],
    post_hook=[
      "{{ log_execution_time(this) }}"  
    ]
) }}

{%- set start_date = var('start_date', none) -%}

{%- if start_date is not none -%}
-- Case: Có start_date → Ưu tiên dữ liệu trong khoảng filter, fallback lấy cùng giờ
WITH all_candidate_data AS (
    SELECT
        province_id,
        time::timestamp AS event_time,
        pm2_5::float AS pm2_5,
        european_aqi_pm2_5::float AS european_aqi_pm2_5,
        insert_time,
        CASE 
            WHEN {{ get_date_filter('time::timestamp') }} THEN 1 
            ELSE 2 
        END as priority_score
    FROM {{ source('raw', 'raw_aq_current') }}
    WHERE extract(hour from time::timestamp) = extract(hour from timestamp '{{ start_date }}')
),

ranked_data AS (
    SELECT 
        province_id,
        event_time,
        pm2_5,
        european_aqi_pm2_5,
        insert_time,
        ROW_NUMBER() OVER (
            PARTITION BY province_id, date_trunc('hour', event_time)
            ORDER BY 
                priority_score ASC,
                insert_time DESC
        ) as rn
    FROM all_candidate_data
)

SELECT 
    province_id,
    event_time,
    pm2_5,
    european_aqi_pm2_5,
    insert_time
FROM ranked_data
WHERE rn = 1

{%- else -%}
-- Case: Không có start_date → Lấy toàn bộ với insert_time cao nhất
SELECT
    province_id,
    time::timestamp AS event_time,
    pm2_5::float AS pm2_5,
    european_aqi_pm2_5::float AS european_aqi_pm2_5,
    insert_time
FROM {{ source('raw', 'raw_aq_current') }}
WHERE insert_time = (SELECT MAX(insert_time) FROM {{ source('raw', 'raw_aq_current') }})

{%- endif -%}