{% macro log_data_quality(columns_to_check=[]) %}
  {% set query %}
    select
      count(*) as total_rows,
      count(distinct province_id) as unique_provinces
      {% for col in columns_to_check %}
      , sum(case when {{ col }} is null then 1 else 0 end) as {{ col }}_nulls
      {% endfor %}
    from {{ this }}
  {% endset %}
  
  {% set results = run_query(query) %}
  
  {% if execute %}
    {% set total = results.columns[0].values()[0] %}
    {% set provinces = results.columns[1].values()[0] %}
    
    {{ log("", info=True) }}
    {{ log("✅ DATA QUALITY CHECK", info=True) }}
    {{ log("  ├─ Total rows: " ~ total, info=True) }}
    {{ log("  ├─ Unique provinces: " ~ provinces ~ " (expected: 63)", info=True) }}
    
    {% for col in columns_to_check %}
      {% set null_count = results.columns[loop.index + 1].values()[0] | default(0, True) %}
      {% if total > 0 %}
        {% set null_pct = (null_count / total * 100) | round(2) %}
      {% else %}
        {% set null_pct = 0 %}
      {% endif %}
      {{ log("  ├─ " ~ col ~ " nulls: " ~ null_count ~ " (" ~ null_pct ~ "%)", info=True) }}
    {% endfor %}
    
    {{ log("  └─ Quality check completed ✓", info=True) }}
  {% endif %}

  select 1 -- satisfy dbt hook execution
{% endmacro %}
