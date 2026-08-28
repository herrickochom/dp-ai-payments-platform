{% materialization iceberg_table, adapter='duckdb' %}
  {%- set target_relation = this.incorporate(type='table') -%}
  {%- set existing_relation = load_relation(target_relation) -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  {%- if existing_relation is not none -%}
    {% call statement('drop_existing_relation', auto_begin=False) %}
      drop table if exists {{ existing_relation }}
    {% endcall %}
  {%- endif -%}

  {# DuckDB's Iceberg REST catalog can create and drop tables, but does not
     implement the ALTER/RENAME operation used by dbt's stock materialization. #}
  {% call statement('main', auto_begin=False) %}
    create table {{ target_relation }} as (
      {{ sql }}
    )
  {% endcall %}

  {{ run_hooks(post_hooks, inside_transaction=False) }}
  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
