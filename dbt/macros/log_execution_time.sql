{% macro log_execution_time(model_name) %}
  {% set columns = adapter.get_columns_in_relation(model_name) %}
  {% set column_names = columns | map(attribute='name') | map('lower') | list %}

  {% set latest_col = none %}
  {% if 'update_at' in column_names %}
    {% set latest_col = 'update_at' %}
  {% elif 'insert_time' in column_names %}
    {% set latest_col = 'insert_time' %}
  {% elif 'event_time' in column_names %}
    {% set latest_col = 'event_time' %}
  {% endif %}

  {% set latest_filter = '' %}
  {% if latest_col %}
    {% set latest_filter %}
      where {{ latest_col }} = (
        select max({{ latest_col }})
        from {{ model_name }}
      )
    {% endset %}
  {% endif %}

  {% set query %}
    select
      count(*) as row_count,
      pg_size_pretty(pg_total_relation_size('{{ model_name }}')) as table_size
    from {{ model_name }}
    {{ latest_filter }}
  {% endset %}
  
  {% set results = run_query(query) %}
  
  {% if execute %}
    {% set row_count = results.columns[0].values()[0] %}
    {% set size = results.columns[1].values()[0] %}
    {% set end_time = modules.datetime.datetime.now() %}
    {% set mode = 'Latest batch only' if latest_col else 'All rows (no timestamp column found)' %}
    
    {{ log("", info=True) }}
    {{ log("⏱️  EXECUTION COMPLETED", info=True) }}
    {{ log("  ├─ Model: " ~ model_name, info=True) }}
    {{ log("  ├─ Mode: " ~ mode, info=True) }}
    {{ log("  ├─ Rows: " ~ row_count, info=True) }}
    {{ log("  ├─ Size: " ~ size, info=True) }}
    {{ log("  └─ Completed at: " ~ end_time, info=True) }}
  {% endif %}

  select 1 -- satisfy dbt hook execution
{% endmacro %}
