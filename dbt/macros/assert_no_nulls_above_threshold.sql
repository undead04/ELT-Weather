{% macro assert_no_nulls_above_threshold(columns_to_check=[], max_null_pct=5) %}
  {#
    Assertion macro: Fails if any column has nulls above threshold percentage
    Usage: 
      post_hook="{{ assert_no_nulls_above_threshold(['temperature_2m', 'pm2_5'], 5) }}"
  #}
  {% set query %}
    SELECT
      COUNT(*) as total_rows
      {% for col in columns_to_check %}
      , SUM(CASE WHEN {{ col }} IS NULL THEN 1 ELSE 0 END) as {{ col }}_nulls
      {% endfor %}
    FROM {{ this }}
  {% endset %}
  
  {% set results = run_query(query) %}
  
  {% if execute %}
    {% set total = results.columns[0].values()[0] %}
    
    {% if total > 0 %}
      {% for col in columns_to_check %}
        {% set null_count = results.columns[loop.index].values()[0] %}
        {% set null_pct = (null_count / total * 100) %}
        
        {% if null_pct > max_null_pct %}
          {{ exceptions.raise_compiler_error(
            "❌ ASSERTION FAILED: Column '" ~ col ~ "' has " ~ null_pct ~ "% nulls (threshold: " ~ max_null_pct ~ "%) in model " ~ this
          ) }}
        {% endif %}
      {% endfor %}
      
      {{ log("✅ Null threshold assertion passed for all columns", info=True) }}
    {% endif %}
  {% endif %}
{% endmacro %}
