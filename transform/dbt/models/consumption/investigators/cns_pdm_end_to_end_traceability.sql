{{ config(materialized='iceberg_table', tags=['consumption', 'audit'], meta={'classification': 'restricted'}) }}

-- Grain: one row per loan. All many-row sources are aggregated to loan_id (or
-- beneficiary_sk for identity) before joining, protecting loan amounts.
with payment_summary as (
    select
        loan_id,
        max(transaction_id) filter (where source_system in ('ICMN_VPM', 'WENDI_PAIN001')) as pain001_transaction_id,
        max(message_id) filter (where source_system in ('ICMN_VPM', 'WENDI_PAIN001')) as pain001_message_id,
        max(transaction_id) filter (where source_system in ('MTN_PACS008', 'AIRTEL_PACS008')) as pacs008_transaction_id,
        max(transaction_id) filter (where source_system = 'WENDI_WALLET') as wallet_transaction_id,
        max(transaction_id) filter (where source_system = 'AGENT') as agent_transaction_id,
        max(instruction_id) as instruction_id,
        max(uetr) as uetr,
        count(*) as payment_count,
        sum(payment_amount) as payment_amount,
        count(*) filter (where transaction_status in ('RJCT', 'FAILED')) as payment_failure_count,
        count(*) filter (where not is_entity_matched) as entity_mismatch_count,
        count(*) filter (where not is_reconciled) as reconciliation_exception_count
    from {{ ref('gld_fct_pdm_payments') }}
    where loan_id is not null
    group by 1
), duplicate_summary as (
    select
        loan_id,
        sum(case when duplicate_indicator then 1 else 0 end) as duplicate_alert_count,
        sum(case when fragmentation_indicator then 1 else 0 end) as fragmentation_alert_count
    from {{ ref('cns_pdm_duplicate_fragmentation_alerts') }}
    group by 1
), lifecycle_summary as (
    select loan_id, count(*) as lifecycle_exception_count
    from {{ ref('cns_pdm_lifecycle_exception_cases') }}
    group by 1
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
    lifecycle.lifecycle_sk,
    loan.loan_id,
    loan.business_plan_id,
    beneficiary.beneficiary_id,
    beneficiary.household_id,
    sacco.sacco_id,
    geography.region,
    geography.district,
    geography.county,
    geography.sub_county,
    geography.parish,
    geography.village,
    loan.project_type,
    special_group.group_name as special_group,
    loan.loan_status,
    loan.disbursement_date,
    loan.as_of_date,
    payment.pain001_message_id,
    payment.pain001_transaction_id,
    payment.pacs008_transaction_id,
    payment.wallet_transaction_id,
    payment.agent_transaction_id,
    payment.instruction_id,
    payment.uetr,
    loan.amount_approved as approved_amount,
    loan.amount_disbursed as disbursed_amount,
    loan.amount_repaid as repaid_amount,
    loan.outstanding_amount,
    loan.repayment_rate,
    coalesce(payment.payment_count, 0) as payment_count,
    payment.payment_amount,
    coalesce(payment.payment_failure_count, 0) as payment_failure_count,
    coalesce(payment.entity_mismatch_count, 0) as entity_mismatch_count,
    coalesce(payment.reconciliation_exception_count, 0) as reconciliation_exception_count,
    coalesce(identity.identity_alert_count, 0) as identity_alert_count,
    coalesce(duplicate.duplicate_alert_count, 0) as duplicate_alert_count,
    coalesce(duplicate.fragmentation_alert_count, 0) as fragmentation_alert_count,
    coalesce(exceptions.lifecycle_exception_count, 0) as lifecycle_exception_count,
    fraud.risk_score,
    fraud.risk_band,
    ai.probability_default_90d,
    ai.ai_risk_band,
    ai.risk_rank,
    coalesce(ai.requires_priority_review, false) as requires_priority_review,
    ai.observation_date as ai_observation_date,
    ai.model_name,
    ai.model_version,
    'INDICATOR_NOT_FRAUD_DETERMINATION' as deterministic_risk_interpretation,
    coalesce(ai.interpretation, 'PREDICTIVE_DEFAULT_RISK_NOT_FRAUD_DETERMINATION') as ai_interpretation
from {{ ref('gld_fct_pdm_loans') }} loan
left join {{ ref('gld_fct_pdm_payment_lifecycle') }} lifecycle using (loan_id)
left join {{ ref('gld_dim_pdm_beneficiary') }} beneficiary
  on loan.beneficiary_sk = beneficiary.beneficiary_sk
left join {{ ref('gld_dim_pdm_sacco') }} sacco
  on loan.sacco_sk = sacco.sacco_sk
left join {{ ref('gld_dim_pdm_geography') }} geography
  on loan.geography_sk = geography.geography_sk
left join {{ ref('gld_dim_pdm_special_group') }} special_group
  on beneficiary.special_group_sk = special_group.special_group_sk
left join payment_summary payment using (loan_id)
left join {{ ref('cns_pdm_beneficiary_identity_alerts') }} identity
  on loan.beneficiary_sk = identity.beneficiary_sk
left join duplicate_summary duplicate using (loan_id)
left join lifecycle_summary exceptions using (loan_id)
left join {{ ref('cns_pdm_fraud_risk_insights') }} fraud using (loan_id)
left join latest_ai ai using (loan_id)
