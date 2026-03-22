{% macro log_row_count(model_name) %}
  {%- set start_date = var('start_date', none) -%}
  {%- set end_date = var('end_date', none) -%}
  {% set query %}
    select count(*) as row_count
    from {{ model_name }}
    where {{ get_date_filter('event_time') }}
  {% endset %}

  {% set results = run_query(query) %}

  {% if execute %}
    {% if results and results.columns[0] %}
      {% set row_count = results.columns[0].values()[0] %}
      {{ log("📊 MODEL DATA LOG: " ~ model_name, info=True) }}

      {% if start_date and end_date %}
        {{ log("  ├─ Mode: Backfill (" ~ start_date ~ " to " ~ end_date ~ ")", info=True) }}
      {% else %}
        {{ log("  ├─ Mode: Incremental (New data only)", info=True) }}
      {% endif %}

      {{ log("  └─ Model has " ~ row_count ~ " rows", info=True) }}
    {% else %}
      {{ log("⚠️ Could not retrieve row count for model " ~ model_name, info=True) }}
    {% endif %}
  {% endif %}

  select 1 -- satisfy dbt hook execution
{% endmacro %}