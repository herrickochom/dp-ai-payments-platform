{% materialization iceberg_table, adapter='duckdb' %}

  {%- set target_relation = api.Relation.create(
      database=target.database,
      schema=this.schema,
      identifier=this.identifier,
      type='table'
  ) -%}

  {%- set existing_relation = load_relation(target_relation) -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  {%- if existing_relation is not none -%}

    {% call statement('drop_existing_relation', auto_begin=False) %}

      drop table if exists {{ target_relation }}

    {% endcall %}

  {%- endif -%}

  {#
    IMPORTANT:
    target.database must resolve to the attached Iceberg catalog alias
    "lakehouse", so generated relations are:

      lakehouse.staging.<model>
      lakehouse.bronze.<model>
      lakehouse.silver.<model>
      lakehouse.gold.<model>
      lakehouse.consumption.<model>

    This ensures tables are registered in Nessie rather than created
    only inside the local DuckDB database.
  #}

  {% call statement('main', auto_begin=False) %}

    create table {{ target_relation }} as (

      {{ sql }}

    )

  {% endcall %}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}