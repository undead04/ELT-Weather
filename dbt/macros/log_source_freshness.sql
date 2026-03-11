{% macro log_source_freshness(source_table) %}
  {% set query %}
    select 
      max(insert_time) as latest_insert,
      min(insert_time) as earliest_insert,
      count(*) as record_count,
      extract(epoch from (current_timestamp - max(insert_time)))/60 as minutes_since_last_insert
    from {{ source_table }}
  {% endset %}
  
  {% set results = run_query(query) %}
  
  {% if execute %}
    {% set latest = results.columns[0].values()[0] %}
    {% set earliest = results.columns[1].values()[0] %}
    {% set count = results.columns[2].values()[0] %}
    {% set minutes = (results.columns[3].values()[0] | default(0, True)) | round(1) %}
    
    {{ log("", info=True) }}
    {{ log("📅 SOURCE FRESHNESS: " ~ source_table, info=True) }}
    {{ log("  ├─ Records: " ~ count, info=True) }}
    {{ log("  ├─ Latest insert: " ~ latest, info=True) }}
    {{ log("  ├─ Earliest insert: " ~ earliest, info=True) }}
    {{ log("  └─ Age: " ~ minutes ~ " minutes ago", info=True) }}
    
    {% if minutes > 30 %}
      {{ log("  ⚠️  WARNING: Data might be stale (>30 minutes old)", info=True) }}
    {% endif %}
  {% endif %}

  select 1 -- satisfy dbt hook execution
{% endmacro %}
