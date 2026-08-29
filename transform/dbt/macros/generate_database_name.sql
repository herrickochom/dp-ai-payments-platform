{% macro generate_database_name(custom_database_name=none, node=none) -%}
  {#
    Nessie's DuckDB Iceberg catalog cannot persist views. Route staging views to
    the local DuckDB database used by profiles.yml; all persisted layers remain
    in the attached `lakehouse` Iceberg catalog.
  #}
  {%- if node is not none and node.resource_type == 'model' and 'staging' in node.fqn -%}
    pdm
  {%- elif custom_database_name is not none -%}
    {{ custom_database_name | trim }}
  {%- else -%}
    {{ target.database }}
  {%- endif -%}
{%- endmacro %}
