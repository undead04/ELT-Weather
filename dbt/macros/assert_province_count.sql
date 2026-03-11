{% macro assert_province_count(expected_count=63) %}
 
  {% set query %}
    SELECT COUNT(DISTINCT province_id) as actual_count
    FROM {{ this }}
  {% endset %}
  
  {% set results = run_query(query) %}
  
  {% if execute %}
    {% set actual = results.columns[0].values()[0] %}
    
    {% if actual != expected_count %}
      {{ exceptions.raise_compiler_error(
        "❌ ASSERTION FAILED: Expected " ~ expected_count ~ " provinces, but found " ~ actual ~ " in model " ~ this
      ) }}
    {% else %}
      {{ log("✅ Province count assertion passed: " ~ actual ~ " provinces", info=True) }}
    {% endif %}
  {% endif %}
{% endmacro %}
