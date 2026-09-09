{{ config(
    materialized='iceberg_table',
    tags=['consumption', 'leadership', 'ai', 'geography', 'default_risk']
) }}

with ai_risk as (

    select *
    from {{ ref('cns_pdm_ai_default_risk') }}

)

select
    observation_date,

    region,
    district,

    count(*) as ai_scored_loan_count,

    avg(probability_default_90d)
        as avg_probability_default_90d,

    max(probability_default_90d)
        as max_probability_default_90d,

    sum(
        case when ai_risk_band = 'LOW'
        then 1 else 0 end
    ) as ai_low_risk_count,

    sum(
        case when ai_risk_band = 'MEDIUM'
        then 1 else 0 end
    ) as ai_medium_risk_count,

    sum(
        case when ai_risk_band = 'HIGH'
        then 1 else 0 end
    ) as ai_high_risk_count,

    sum(
        case when ai_risk_band = 'SEVERE'
        then 1 else 0 end
    ) as ai_severe_risk_count,

    sum(
        case when ai_risk_band in ('HIGH', 'SEVERE')
        then 1 else 0 end
    ) as ai_priority_review_count,

    avg(
        case when ai_risk_band in ('HIGH', 'SEVERE')
        then 1.0 else 0.0 end
    ) as ai_priority_review_rate,

    min(risk_rank) as highest_ai_risk_rank,

    max(model_name) as model_name,
    max(model_version) as model_version,

    'PREDICTIVE_DEFAULT_RISK_NOT_FRAUD_DETERMINATION'
        as interpretation

from ai_risk

group by
    observation_date,
    region,
    district
