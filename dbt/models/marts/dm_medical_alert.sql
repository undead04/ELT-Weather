{{
  config(
    materialized = 'incremental',
    tags=['current_flow','forecast_flow'],
    unique_key = ['province_id', 'date_key', 'time_key', 'alert_type'],
    post_hook=[
      "{{ log_row_count(this) }}",
      "{{ log_execution_time(this) }}"
    ]
  )
}}

SELECT 
    province_id,
    date_key,
    time_key,
    event_time,
    province_name,
    risk_type,
    risk_level,
    recommendation,
    affected_population,
    alert_type,
    CURRENT_TIMESTAMP as update_at
FROM {{ ref('int_medical_alert_actual') }} a
WHERE 1 = 1
  AND {{ get_date_filter('event_time') }}
    {% if is_incremental() and not var('is_backfill', false) %}
        AND a.update_at > (
            SELECT COALESCE(MAX(update_at), '1900-01-01') FROM {{ this }} WHERE alert_type = 'Actual'
        )
    {% endif %}
UNION ALL
SELECT 
    province_id,
    date_key,
    time_key,
    event_time,
    province_name,
    risk_type,
    risk_level,
    recommendation,
    affected_population,
    alert_type,
    CURRENT_TIMESTAMP as update_at
FROM {{ ref('int_medical_alert_forecast') }} f
WHERE 1 = 1
  AND {{ get_date_filter('event_time') }}
    {% if is_incremental() and not var('is_backfill', false) %}
        AND f.update_at > (
            SELECT COALESCE(MAX(update_at), '1900-01-01') FROM {{ this }} WHERE alert_type = 'Forecast'
        )
    {% endif %}