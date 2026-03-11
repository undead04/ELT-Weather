{% macro log_row_count(model_name) %}
  -- 1. Luôn để query ngoài block if execute để dbt vẫn thấy phụ thuộc (nếu có)
  {% set query %}
    select count(*) as row_count from {{ model_name }}
  {% endset %}
  
  -- 2. Chỉ chạy query và xử lý kết quả khi dbt đang ở phase EXECUTE
  {% if execute %}
    {% set results = run_query(query) %}
    
    {% if results and results.columns[0] %}
        {% set row_count = results.columns[0].values()[0] %}
        {{ log("📊 Model " ~ model_name ~ " has " ~ (row_count | default(0)) ~ " rows", info=True) }}
    {% else %}
        {{ log("⚠️ Could not retrieve row count for " ~ model_name, info=True) }}
    {% endif %}
  {% endif %}

  select 1 -- satisfy dbt hook execution
{% endmacro %}