{% macro log_raw_record_count(source_name, table_name, event_col) %}
  {%- set start_date = var('start_date', none) -%}
  {%- set end_date = var('end_date', none) -%}
  {% set source_table = source(source_name, table_name) %}
  {% set query %}
    select count(*) as row_count 
    from {{ source_table }} r
    where {{ get_date_filter(event_col) }}
  {% endset %}

  {% set results = run_query(query) %}

  {% if execute %}
    {% set row_count = results.columns[0].values()[0] %}
    {{ log("📥 SOURCE DATA LOG: " ~ table_name, info=True) }}
    
    {% if start_date and end_date %}
      {{ log("  ├─ Mode: Backfill (" ~ start_date ~ " to " ~ end_date ~ ")", info=True) }}
    {% else %}
      {{ log("  ├─ Mode: Incremental (New data only)", info=True) }}
    {% endif %}
    
    {{ log("  └─ Found " ~ row_count ~ " records to process", info=True) }}
  {% endif %}

  select 1 -- satisfy dbt hook execution
{% endmacro %}
