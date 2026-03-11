{% macro log_incremental_stats() %}
  {% if is_incremental() %}
    {% set query %}
      select count(*) as existing_rows from {{ this }}
    {% endset %}
    
    {% set results = run_query(query) %}
    
    {% if execute %}
      {% set existing = results.columns[0].values()[0] %}
      
      {{ log("", info=True) }}
      {{ log("🔄 INCREMENTAL UPDATE STATS", info=True) }}
      {{ log("  ├─ Existing rows: " ~ existing, info=True) }}
      {{ log("  └─ Running incremental merge...", info=True) }}
    {% endif %}
  {% else %}
    {{ log("🔃 FULL REFRESH - All data will be reloaded", info=True) }}
  {% endif %}
{% endmacro %}
