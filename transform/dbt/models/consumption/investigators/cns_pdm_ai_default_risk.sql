{{ config(
    materialized='iceberg_table',
    tags=['consumption', 'ai', 'ml', 'risk', 'investigation']
) }}

select
    snapshot_id,
    beneficiary_id,
    loan_id,
    sacco_id,

    region,
    district,
    county,
    sub_county,
    parish,
    village,

    project_type,
    special_group,

    observation_date,

    probability_default_90d,
    ai_risk_band,
    risk_rank,

    model_name,
    model_version,
    scored_at_utc,

    case
        when ai_risk_band in ('HIGH', 'SEVERE') then true
        else false
    end as requires_priority_review,

    'PREDICTIVE_DEFAULT_RISK_NOT_FRAUD_DETERMINATION' as interpretation

from {{ ref('br_pdm_ai_default_risk') }}
