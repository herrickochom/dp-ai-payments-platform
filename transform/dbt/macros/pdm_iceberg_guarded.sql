{% materialization pdm_iceberg_guarded, adapter='bigquery' %}
 {{ exceptions.raise_compiler_error('Managed Iceberg publication not certified or authorised') }}
{% endmaterialization %}
