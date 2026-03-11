{% macro assert_no_duplicates(unique_columns=[]) %}
  {#
    Assertion macro: Fails if duplicate records exist based on unique columns
    Usage:
      post_hook="{{ assert_no_duplicates(['province_id', 'date_key', 'time_key']) }}"
  #}
  {% set columns_str = unique_columns | join(', ') %}
  
  {% set query %}
    SELECT 
      {{ columns_str }},
      COUNT(*) as duplicate_count
    FROM {{ this }}
    GROUP BY {{ columns_str }}
    HAVING COUNT(*) > 1
  {% endset %}
  
  {% set results = run_query(query) %}
  
  {% if execute %}
    {% if results.rows | length > 0 %}
      {% set duplicate_rows = results.rows | length %}
      {{ exceptions.raise_compiler_error(
        "❌ ASSERTION FAILED: Found " ~ duplicate_rows ~ " duplicate records in model " ~ this ~ " based on columns [" ~ columns_str ~ "]"
      ) }}
    {% else %}
      {{ log("✅ No duplicates found for columns: " ~ columns_str, info=True) }}
    {% endif %}
  {% endif %}
{% endmacro %}
