{% macro assert_values_in_range(column_name, min_value, max_value, allow_nulls=true) %}
  {#
    Assertion macro: Fails if column values are outside the specified range
    Usage:
      post_hook="{{ assert_values_in_range('temperature_2m', -50, 60, true) }}"
  #}
  {% set null_filter = "IS NOT NULL" if not allow_nulls else "IS NOT NULL" %}
  
  {% set query %}
    SELECT 
      COUNT(*) as violations,
      MIN({{ column_name }}) as min_found,
      MAX({{ column_name }}) as max_found
    FROM {{ this }}
    WHERE {{ column_name }} IS NOT NULL
      AND ({{ column_name }} < {{ min_value }} OR {{ column_name }} > {{ max_value }})
  {% endset %}
  
  {% set results = run_query(query) %}
  
  {% if execute %}
    {% set violations = results.columns[0].values()[0] %}
    
    {% if violations > 0 %}
      {% set min_found = results.columns[1].values()[0] %}
      {% set max_found = results.columns[2].values()[0] %}
      
      {{ exceptions.raise_compiler_error(
        "❌ ASSERTION FAILED: Column '" ~ column_name ~ "' has " ~ violations ~ " value(s) outside range [" ~ min_value ~ ", " ~ max_value ~ "]. Found range: [" ~ min_found ~ ", " ~ max_found ~ "] in model " ~ this
      ) }}
    {% else %}
      {{ log("✅ Range assertion passed for '" ~ column_name ~ "' [" ~ min_value ~ ", " ~ max_value ~ "]", info=True) }}
    {% endif %}
  {% endif %}
{% endmacro %}
