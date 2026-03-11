{% macro log_aggregation_summary(group_by_col, measure_col) %}
  {% set query %}
    select 
      min({{ measure_col }}) as min_value,
      max({{ measure_col }}) as max_value,
      avg({{ measure_col }}) as avg_value,
      count(distinct {{ group_by_col }}) as unique_groups
    from {{ this }}
    where {{ measure_col }} is not null
  {% endset %}
  
  {% set results = run_query(query) %}
  
  {% if execute %}
    {% set min_val = (results.columns[0].values()[0] | default(0, True)) | round(2) %}
    {% set max_val = (results.columns[1].values()[0] | default(0, True)) | round(2) %}
    {% set avg_val = (results.columns[2].values()[0] | default(0, True)) | round(2) %}
    {% set groups = results.columns[3].values()[0] %}
    
    {{ log("", info=True) }}
    {{ log("📈 AGGREGATION SUMMARY", info=True) }}
    {{ log("  ├─ Grouped by: " ~ group_by_col, info=True) }}
    {{ log("  ├─ Unique groups: " ~ groups, info=True) }}
    {{ log("  ├─ " ~ measure_col ~ " range: [" ~ min_val ~ ", " ~ max_val ~ "]", info=True) }}
    {{ log("  └─ " ~ measure_col ~ " average: " ~ avg_val, info=True) }}
  {% endif %}

  select 1 -- satisfy dbt hook execution
{% endmacro %}
