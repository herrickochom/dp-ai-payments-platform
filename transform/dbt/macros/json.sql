{% macro extract_json(json_column, json_path) %}
 json_value({{ json_column }}, '{{ json_path }}')
{% endmacro %}
