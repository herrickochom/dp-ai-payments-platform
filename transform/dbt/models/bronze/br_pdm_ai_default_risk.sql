{{ config(materialized='pdm_iceberg_guarded', tags=['bronze', 'ai', 'ml', 'default_risk']) }}

select
    cast(snapshot_id as string) as snapshot_id,
    cast(beneficiary_id as string) as beneficiary_id,
    cast(loan_id as string) as loan_id,
    cast(sacco_id as string) as sacco_id,

    cast(region as string) as region,
    cast(district as string) as district,
    cast(county as string) as county,
    cast(sub_county as string) as sub_county,
    cast(parish as string) as parish,
    cast(village as string) as village,

    cast(project_type as string) as project_type,
    cast(special_group as string) as special_group,

    cast(observation_date as date) as observation_date,

    cast(probability_default_90d as float64) as probability_default_90d,
    cast(ai_risk_band as string) as ai_risk_band,

    cast(model_name as string) as model_name,
    cast(model_version as string) as model_version,
    cast(scored_at_utc as timestamp) as scored_at_utc,

    cast(risk_rank as int64) as risk_rank

from {{ source('pdm_ai_input','default_risk_current_predictions') }}
