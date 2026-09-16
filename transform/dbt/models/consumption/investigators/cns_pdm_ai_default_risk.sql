{{ config(
    materialized='iceberg_table',
    tags=['consumption', 'ai', 'ml', 'risk', 'investigation', 'privacy-boundary']
) }}

-- GATE 2 PRIVACY BOUNDARY: pseudonymous by default. The canonical analytical
-- beneficiary identifier is beneficiary_token (resolved at the Silver privacy
-- boundary from the loan linkage). Only approved coarse geography is carried.
-- The predictive-risk population and its semantics are unchanged.

select
    risk.snapshot_id,
    loan_link.beneficiary_token,
    risk.loan_id,
    risk.sacco_id,

    risk.region,
    risk.district,
    risk.county,
    risk.sub_county,

    risk.project_type,
    risk.special_group,

    risk.observation_date,

    risk.probability_default_90d,
    risk.ai_risk_band,
    risk.risk_rank,

    risk.model_name,
    risk.model_version,
    risk.scored_at_utc,

    case
        when risk.ai_risk_band in ('HIGH', 'SEVERE') then true
        else false
    end as requires_priority_review,

    'PREDICTIVE_DEFAULT_RISK_NOT_FRAUD_DETERMINATION' as interpretation

from {{ ref('br_pdm_ai_default_risk') }} risk
left join {{ ref('slv_pdm_loan_beneficiary_links') }} loan_link
    on risk.loan_id = loan_link.loan_id
