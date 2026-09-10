{{ config(materialized='iceberg_table', tags=['consumption', 'intervention', 'operations']) }}

-- Grain: one row per loan. Payment and AI inputs are reduced to one row per
-- loan before joining, preventing multiplication of loan financial amounts.
with payment_metrics as (
    select
        loan_id,
        count(*) filter (where is_failed) as payment_failure_count,
        count(*) filter (where is_unreconciled) as unreconciled_payment_count,
        count(*) filter (where not is_entity_matched) as unmatched_payment_count,
        count(*) filter (where account_substitution_indicator) as account_substitution_count
    from {{ ref('cns_pdm_payment_operations') }}
    where loan_id is not null
    group by 1
), latest_ai as (
    select
        loan_id,
        probability_default_90d,
        ai_risk_band,
        risk_rank,
        requires_priority_review,
        observation_date,
        model_name,
        model_version,
        scored_at_utc,
        interpretation
    from {{ ref('cns_pdm_ai_default_risk') }}
    qualify row_number() over (
        partition by loan_id order by observation_date desc, scored_at_utc desc, snapshot_id desc
    ) = 1
)
select
    lifecycle.lifecycle_sk,
    loan.loan_sk,
    loan.loan_id,
    beneficiary.beneficiary_id,
    sacco.sacco_id,
    sacco.sacco_name,
    geography.region,
    geography.district,
    geography.county,
    geography.sub_county,
    geography.parish,
    geography.village,
    loan.project_type,
    loan.loan_status,
    loan.amount_approved as approved_amount,
    loan.amount_disbursed as disbursed_amount,
    loan.amount_repaid as repaid_amount,
    loan.outstanding_amount,
    loan.repayment_rate,
    loan.repayment_status,
    loan.days_past_due,
    loan.delinquency_bucket,
    case
        when loan.disbursement_date is null then null
        else date_diff('day', cast(loan.disbursement_date as date), coalesce(cast(loan.as_of_date as date), current_date))
    end as loan_age_days,
    case
        when loan.disbursement_date is null then null
        else date_diff('month', cast(loan.disbursement_date as date), coalesce(cast(loan.as_of_date as date), current_date))
    end as months_since_disbursement,
    coalesce(payment.payment_failure_count, 0) as payment_failure_count,
    coalesce(payment.unreconciled_payment_count, 0) as unreconciled_payment_count,
    coalesce(payment.unmatched_payment_count, 0) as unmatched_payment_count,
    coalesce(payment.account_substitution_count, 0) as account_substitution_count,
    lifecycle.approved_control_status,
    lifecycle.instructed_control_status,
    lifecycle.sent_status_control_status,
    lifecycle.settled_control_status,
    lifecycle.credited_control_status,
    lifecycle.cashout_control_status,
    lifecycle.repaid_control_status,
    lifecycle.approved_to_instructed_variance,
    lifecycle.instructed_to_settled_variance,
    lifecycle.settled_to_credited_variance,
    lifecycle.disbursed_to_credited_variance,
    lifecycle.credited_to_cashout_variance,
    lifecycle.rejected_status_count,
    lifecycle.instruction_count,
    lifecycle.settlement_channel_count,
    lifecycle.cashout_count,
    ai.probability_default_90d,
    ai.ai_risk_band,
    ai.risk_rank,
    coalesce(ai.requires_priority_review, false) as requires_priority_review,
    ai.observation_date as ai_observation_date,
    ai.model_name,
    ai.model_version,
    ai.scored_at_utc,
    coalesce(ai.interpretation, 'PREDICTIVE_DEFAULT_RISK_NOT_FRAUD_DETERMINATION') as ai_interpretation,
    case
        when lifecycle.approved_control_status = 'MISSING'
          or lifecycle.instructed_control_status = 'MISSING'
          or lifecycle.settled_control_status in ('MISSING', 'REJECTED')
          or lifecycle.credited_control_status = 'MISSING'
          or coalesce(payment.payment_failure_count, 0) > 0
          or coalesce(payment.unreconciled_payment_count, 0) > 0
          or coalesce(ai.requires_priority_review, false) then 'REQUIRES_INTERVENTION'
        else 'PROGRESSING'
    end as intervention_status,
    case
        when lifecycle.approved_control_status = 'MISSING'
          or lifecycle.instructed_control_status = 'MISSING'
          or lifecycle.settled_control_status in ('MISSING', 'REJECTED')
          or lifecycle.credited_control_status = 'MISSING'
          or coalesce(payment.payment_failure_count, 0) > 0
          or coalesce(payment.unreconciled_payment_count, 0) > 0 then 'REVIEW_OPERATIONAL_EXCEPTION'
        when coalesce(ai.requires_priority_review, false) then 'PRIORITISE_REPAYMENT_SUPPORT_REVIEW'
        when loan.outstanding_amount > 0 then 'MONITOR_REPAYMENT'
        else 'NO_CURRENT_ACTION'
    end as recommended_operational_action
from {{ ref('gld_fct_pdm_loans') }} loan
left join {{ ref('gld_dim_pdm_beneficiary') }} beneficiary using (beneficiary_sk)
left join {{ ref('gld_dim_pdm_sacco') }} sacco using (sacco_sk)
left join {{ ref('gld_dim_pdm_geography') }} geography
  on loan.geography_sk = geography.geography_sk
left join {{ ref('gld_fct_pdm_payment_lifecycle') }} lifecycle using (loan_id)
left join payment_metrics payment using (loan_id)
left join latest_ai ai using (loan_id)
