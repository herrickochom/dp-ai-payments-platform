{{ config(materialized='iceberg_table', tags=['consumption', 'risk', 'investigation']) }}

with patterns as (
    select loan_id,
        bool_or(has_duplicate_entitlement) as duplicate_payment_flag,
        bool_or(has_fragmented_payment) as fragmentation_flag
    from {{ ref('cns_pdm_duplicate_fragmentation_alerts') }} group by 1
), cashouts as (
    select loan_id,
        bool_or(days_from_loan_approval between 0 and 1) as rapid_cashout_flag,
        bool_or(exceeds_approved_amount) as amount_mismatch_flag
    from {{ ref('gld_fct_pdm_agent_cashouts') }} group by 1
), latest_ai as (
    select loan_id, probability_default_90d, ai_risk_band, risk_rank,
        requires_priority_review, observation_date, model_name, model_version,
        interpretation
    from {{ ref('cns_pdm_ai_default_risk') }}
    qualify row_number() over (
        partition by loan_id order by observation_date desc, scored_at_utc desc, snapshot_id desc
    ) = 1
)
select
    lifecycle.lifecycle_sk as risk_case_sk,
    lifecycle.loan_id,
    lifecycle.beneficiary_sk,
    beneficiary.beneficiary_id,
    lifecycle.sacco_sk,
    sacco.sacco_id,
    lifecycle.geography_sk,
    loan_geography.region,
    loan_geography.district,
    loan_geography.parish,
    coalesce(patterns.duplicate_payment_flag, false) as duplicate_payment_flag,
    coalesce(patterns.fragmentation_flag, false) as fragmentation_flag,
    coalesce(identity.identity_alert_count, 0) > 0 as beneficiary_identity_flag,
    coalesce(cashouts.rapid_cashout_flag, false) as rapid_cashout_flag,
    geographic.geographic_risk_band = 'HIGH' as geographic_anomaly_flag,
    coalesce(cashouts.amount_mismatch_flag, false)
      or coalesce(lifecycle.approved_to_instructed_variance <> 0, false)
      or coalesce(lifecycle.instructed_to_settled_variance <> 0, false)
      or coalesce(lifecycle.settled_to_credited_variance <> 0, false)
        as amount_mismatch_flag,
    lifecycle.intervention_priority = 'HIGH' as lifecycle_exception_flag,
    coalesce(identity.has_account_substitution, false) as account_substitution_flag,
    (case when coalesce(patterns.duplicate_payment_flag, false) then 20 else 0 end
     + case when coalesce(patterns.fragmentation_flag, false) then 20 else 0 end
     + case when coalesce(identity.identity_alert_count, 0) > 0 then 15 else 0 end
     + case when coalesce(cashouts.rapid_cashout_flag, false) then 10 else 0 end
     + case when geographic.geographic_risk_band = 'HIGH' then 15 else 0 end
     + case when lifecycle.intervention_priority = 'HIGH' then 20 else 0 end) as risk_score,
    case
        when lifecycle.intervention_priority = 'HIGH'
          or coalesce(patterns.duplicate_payment_flag, false)
          or coalesce(patterns.fragmentation_flag, false) then 'HIGH'
        when coalesce(identity.identity_alert_count, 0) > 0
          or coalesce(cashouts.rapid_cashout_flag, false)
          or geographic.geographic_risk_band = 'HIGH' then 'MEDIUM'
        else 'LOW' end as risk_band,
    lifecycle.intervention_priority,
    'INDICATOR_NOT_FRAUD_DETERMINATION' as interpretation,
    ai.probability_default_90d,
    ai.ai_risk_band,
    ai.risk_rank,
    coalesce(ai.requires_priority_review, false) as requires_priority_review,
    ai.observation_date as ai_observation_date,
    ai.model_name,
    ai.model_version,
    coalesce(ai.interpretation, 'PREDICTIVE_DEFAULT_RISK_NOT_FRAUD_DETERMINATION')
        as ai_interpretation
from {{ ref('cns_pdm_lifecycle_exceptions') }} lifecycle
left join {{ ref('cns_pdm_beneficiary_identity_alerts') }} identity
  on lifecycle.beneficiary_sk = identity.beneficiary_sk
left join {{ ref('gld_dim_pdm_geography') }} loan_geography
  on lifecycle.geography_sk = loan_geography.geography_sk
left join {{ ref('cns_pdm_parish_geographic_risk') }} geographic
  on loan_geography.region = geographic.region
 and loan_geography.district = geographic.district
 and loan_geography.parish = geographic.parish
left join patterns on lifecycle.loan_id = patterns.loan_id
left join cashouts on lifecycle.loan_id = cashouts.loan_id
left join {{ ref('gld_dim_pdm_beneficiary') }} beneficiary
  on lifecycle.beneficiary_sk = beneficiary.beneficiary_sk
left join {{ ref('gld_dim_pdm_sacco') }} sacco
  on lifecycle.sacco_sk = sacco.sacco_sk
left join latest_ai ai on lifecycle.loan_id = ai.loan_id
