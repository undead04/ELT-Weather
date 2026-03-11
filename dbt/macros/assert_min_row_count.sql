{% macro assert_min_row_count(min_rows=1) %}
  {#
    Assertion macro: Fails if model has fewer rows than expected minimum
    Useful to detect empty or nearly empty tables
    Usage:
      post_hook="{{ assert_min_row_count(100) }}"
  #}
  {% set query %}
    SELECT COUNT(*) as row_count
    FROM {{ this }}
  {% endset %}
  
  {% set results = run_query(query) %}
  
  {% if execute %}
    {% set actual_rows = results.columns[0].values()[0] %}
    
    {% if actual_rows < min_rows %}
      {{ exceptions.raise_compiler_error(
        "❌ ASSERTION FAILED: Model " ~ this ~ " has only " ~ actual_rows ~ " rows (minimum required: " ~ min_rows ~ ")"
      ) }}
    {% else %}
      {{ log("✅ Minimum row count assertion passed: " ~ actual_rows ~ " rows (min: " ~ min_rows ~ ")", info=True) }}
    {% endif %}
  {% endif %}
{% endmacro %}
