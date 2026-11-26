
{% materialization pdm_iceberg_pilot, adapter='bigquery' %}

    {% set allowed = ["br_pdm_pdmis_beneficiaries", "br_pdm_pdmis_business_plans", "br_pdm_pdmis_loans", "br_pdm_pdmis_repayments", "br_pdm_icmn_vpm_pain001", "br_pdm_cpo_psn_pain002", "br_pdm_payments_pmn_lifecycle_events", "br_pdm_payments_plm_lifecycle_events", "br_pdm_agent_locations", "br_pdm_agent_transactions"] %}

    {% if env_var('PDM_ICEBERG_APPROVED', 'false') != 'true' %}
        {{ exceptions.raise_compiler_error('Deployment not approved') }}
    {% endif %}

    {% if this.identifier not in allowed
          or this.schema != 'bronze'
          or this.database != 'dp-pdm-ai-dev-0b4da804' %}
        {{ exceptions.raise_compiler_error('Unauthorised target') }}
    {% endif %}

    {% if load_relation(this) is not none %}
        {{ exceptions.raise_compiler_error('Target exists; overwrite blocked') }}
    {% endif %}

    {% set target_relation = this.incorporate(type='table') %}

    {% call statement('main') %}
        CREATE TABLE {{ target_relation }}
        WITH CONNECTION `dp-pdm-ai-dev-0b4da804.europe-west2.pdm-iceberg`
        OPTIONS (
            file_format = 'PARQUET',
            table_format = 'ICEBERG',
            storage_uri = 'gs://dp-pdm-ai-dev-0b4da804-iceberg/bronze/{{ this.identifier }}/'
        )
        AS
        {{ sql }}
    {% endcall %}

    {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
