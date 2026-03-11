{% macro get_date_filter(event_col) %}
  {%- set start_date = var('start_date', none) -%}
  {%- set end_date = var('end_date', none) -%}

  {%- if start_date is not none and end_date is not none -%}
    {{ event_col }} >= timestamp '{{ start_date }}' 
    and {{ event_col }} < timestamp '{{ end_date }}'
  {%- else -%}
    1=1
  {%- endif -%}
{% endmacro %}
