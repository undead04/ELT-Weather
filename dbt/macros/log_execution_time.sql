{% macro log_execution_time(model_name) %}
  {% set query %}
    select 
      count(*) as row_count,
      pg_size_pretty(pg_total_relation_size('{{ model_name }}')) as table_size
    from {{ model_name }}
  {% endset %}
  
  {% set results = run_query(query) %}
  
  {% if execute %}
    {% set row_count = results.columns[0].values()[0] %}
    {% set size = results.columns[1].values()[0] %}
    {% set end_time = modules.datetime.datetime.now() %}
    
    {{ log("", info=True) }}
    {{ log("⏱️  EXECUTION COMPLETED", info=True) }}
    {{ log("  ├─ Model: " ~ model_name, info=True) }}
    {{ log("  ├─ Rows: " ~ row_count, info=True) }}
    {{ log("  ├─ Size: " ~ size, info=True) }}
    {{ log("  └─ Completed at: " ~ end_time, info=True) }}
  {% endif %}

  select 1 -- satisfy dbt hook execution
{% endmacro %}
