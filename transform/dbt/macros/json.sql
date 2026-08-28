{% macro extract_json(json_column, json_path) %}
  json_extract_string({{ json_column }}, '{{ json_path }}')
{% endmacro %}
