{{ config(
    materialized='iceberg_table',
    tags=['bronze', 'ai', 'ml', 'default_risk']
) }}

select
    cast(snapshot_id as varchar) as snapshot_id,
    cast(beneficiary_id as varchar) as beneficiary_id,
    cast(loan_id as varchar) as loan_id,
    cast(sacco_id as varchar) as sacco_id,

    cast(region as varchar) as region,
    cast(district as varchar) as district,
    cast(county as varchar) as county,
    cast(sub_county as varchar) as sub_county,
    cast(parish as varchar) as parish,
    cast(village as varchar) as village,

    cast(project_type as varchar) as project_type,
    cast(special_group as varchar) as special_group,

    cast(observation_date as date) as observation_date,

    cast(probability_default_90d as double) as probability_default_90d,
    cast(ai_risk_band as varchar) as ai_risk_band,

    cast(model_name as varchar) as model_name,
    cast(model_version as varchar) as model_version,
    cast(scored_at_utc as timestamptz) as scored_at_utc,

    cast(risk_rank as bigint) as risk_rank

from read_json_auto(
    '/app/data/pdmis_ml/default_risk_current_predictions.jsonl',
    format = 'newline_delimited'
)
