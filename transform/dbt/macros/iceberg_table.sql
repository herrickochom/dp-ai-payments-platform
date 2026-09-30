{% materialization iceberg_table, adapter='duckdb' %}

  {%- set target_relation = api.Relation.create(
      database='lakehouse',
      schema=this.schema,
      identifier=this.identifier,
      type='table'
  ) -%}

  {%- set existing_relation = load_relation(target_relation) -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  {%- if existing_relation is not none -%}

    {#
      PRODUCTION SAFETY INVARIANT

      Replacement of a published Iceberg table is intentionally blocked.

      DROP + CREATE is not an atomic publication operation against the
      attached Iceberg REST catalogue. If CREATE fails after DROP, the
      previously published relation can disappear.

      A future implementation may replace this guard only after isolated
      Nessie reference/branch execution and controlled promotion have been
      implemented and integration-tested against the deployed catalogue.
    #}

    {{ exceptions.raise_compiler_error(
        "Unsafe Iceberg replacement blocked for " ~ target_relation
        ~ ". Existing published tables must not be dropped before a "
        ~ "replacement has been safely staged and promoted."
    ) }}

  {%- endif -%}

  {#
    IMPORTANT:
    The controlled connection plugin attaches the Iceberg catalog using the
    fixed alias "lakehouse", so generated relations are:

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

{#
  Trino implementation.

  Publication remains fail-closed. Transform execution occurs against an
  execution-scoped Nessie branch, so a model must not replace an existing
  relation in place. A fresh relation is created using dbt-trino's native
  create_table_as implementation.
#}
{% materialization iceberg_table, adapter='trino' %}

  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set target_relation = this.incorporate(type='table') -%}

  {{ run_hooks(pre_hooks) }}

  {%- if existing_relation is not none -%}
    {{ exceptions.raise_compiler_error(
        "Unsafe Iceberg replacement blocked for " ~ target_relation
        ~ ". Existing published tables must not be replaced in place."
    ) }}
  {%- endif -%}

  {% call statement('main') -%}
    {{ create_table_as(False, target_relation, sql) }}
  {%- endcall %}

  {% do persist_docs(target_relation, model) %}

  {%- set grant_config = config.get('grants') -%}
  {%- set should_revoke = should_revoke(
      existing_relation,
      full_refresh_mode=True
  ) -%}
  {% do apply_grants(
      target_relation,
      grant_config,
      should_revoke=should_revoke
  ) %}

  {{ run_hooks(post_hooks) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}

