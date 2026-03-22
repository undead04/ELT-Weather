{{
  config(
    materialized = 'incremental',
    tags=['current_flow','forecast_flow'],
    unique_key = ['province_id', 'date_key', 'time_key', 'alert_type'],
    post_hook=[
      "{{ log_row_count(ref('int_medical_alert_actual')) }}",
      "{{ log_row_count(ref('int_medical_alert_forecast')) }}",
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