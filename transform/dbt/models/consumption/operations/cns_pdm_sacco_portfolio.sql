{{ config(materialized='iceberg_table', tags=['consumption', 'sacco', 'operations']) }}

-- Grain: one row per SACCO. Every many-side source is aggregated to sacco_sk
-- before joining so loan financial measures cannot fan out.
with loan_metrics as (
    select
        sacco_sk,
        count(*) as loan_count,
        count(distinct beneficiary_sk) as beneficiary_count,
        sum(amount_requested) as requested_amount,
        sum(amount_approved) as approved_amount,
        sum(amount_disbursed) as disbursed_amount,
        sum(amount_repaid) as repaid_amount,
        sum(principal_repaid) as principal_repaid_amount,
        sum(outstanding_amount) as outstanding_amount,
        sum(undisbursed_amount) as undisbursed_amount,
        count(*) filter (where outstanding_amount > 0) as loans_with_outstanding_balance
    from {{ ref('gld_fct_pdm_loans') }}
    group by 1
), payment_metrics as (
    select
        sacco_sk,
        count(*) filter (where is_failed) as payment_failure_count,
        count(*) filter (where is_failed or is_unreconciled or not is_entity_matched)
            as operational_exception_count
    from {{ ref('cns_pdm_payment_operations') }}
    group by 1
), latest_ai as (
    select loan_id, ai_risk_band, requires_priority_review
    from {{ ref('cns_pdm_ai_default_risk') }}
    qualify row_number() over (
        partition by loan_id order by observation_date desc, scored_at_utc desc, snapshot_id desc
    ) = 1
), ai_metrics as (
    select
        loan.sacco_sk,
        count(*) filter (where ai.ai_risk_band in ('HIGH', 'SEVERE')) as high_risk_loan_count,
        count(*) filter (where ai.requires_priority_review) as ai_priority_review_count
    from {{ ref('gld_fct_pdm_loans') }} loan
    left join latest_ai ai using (loan_id)
    group by 1
)
select
    sacco.sacco_sk,
    sacco.geography_sk,
    sacco.sacco_id,
    sacco.sacco_name,
    geography.region,
    geography.district,
    sacco.office_exists,
    sacco.is_active,
    coalesce(loan.loan_count, 0) as loan_count,
    coalesce(loan.beneficiary_count, 0) as beneficiary_count,
    loan.requested_amount,
    loan.approved_amount,
    loan.disbursed_amount,
    loan.repaid_amount,
    loan.principal_repaid_amount,
    loan.outstanding_amount,
    loan.undisbursed_amount,
    loan.principal_repaid_amount / nullif(loan.disbursed_amount, 0) as principal_repayment_rate,
    loan.disbursed_amount / nullif(loan.approved_amount, 0) as disbursement_rate,
    coalesce(loan.loans_with_outstanding_balance, 0) as loans_with_outstanding_balance,
    coalesce(payment.payment_failure_count, 0) as payment_failure_count,
    coalesce(payment.operational_exception_count, 0) as operational_exception_count,
    coalesce(ai.high_risk_loan_count, 0) as high_risk_loan_count,
    coalesce(ai.ai_priority_review_count, 0) as ai_priority_review_count
from {{ ref('gld_dim_pdm_sacco') }} sacco
left join {{ ref('gld_dim_pdm_geography') }} geography using (geography_sk)
left join loan_metrics loan using (sacco_sk)
left join payment_metrics payment using (sacco_sk)
left join ai_metrics ai using (sacco_sk)
where sacco.sacco_id is not null
