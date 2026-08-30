{{ config(materialized='iceberg_table', tags=['consumption', 'executive']) }}

with loans as (
    select
        sum(amount_approved) as total_approved_amount,
        sum(amount_disbursed) as total_disbursed_amount,
        sum(amount_repaid) as total_repaid_amount,
        sum(outstanding_amount) as total_outstanding_amount,
        count(*) as loan_count,
        count(*) filter (where outstanding_amount > 0) as active_loan_count,
        count(distinct beneficiary_sk) as beneficiary_count,
        count(distinct sacco_sk) as sacco_count
    from {{ ref('gld_fct_pdm_loans') }}
), lifecycle as (
    select
        sum(instructed_amount) as total_instructed_amount,
        sum(settled_amount) as total_settled_amount,
        sum(credited_amount) as total_credited_amount,
        sum(cashout_amount) as total_cashout_amount,
        count(*) filter (where intervention_priority <> 'LOW') as lifecycle_exception_count
    from {{ ref('cns_pdm_lifecycle_exceptions') }}
), payments as (
    select
        count(*) as payment_count,
        count(*) filter (where is_reconciled) as reconciled_payment_count,
        count(*) filter (where not is_entity_matched) as unmatched_payment_count
    from {{ ref('gld_fct_pdm_payments') }}
), risks as (
    select count(*) filter (where identity_risk_band = 'HIGH') as high_risk_case_count
    from {{ ref('cns_pdm_beneficiary_identity_alerts') }}
), parishes as (
    select
        count(*) filter (where geographic_risk_band = 'LOW') as parishes_on_track,
        count(*) filter (where geographic_risk_band = 'MEDIUM') as parishes_at_risk,
        count(*) filter (where geographic_risk_band = 'HIGH') as parishes_critical
    from {{ ref('cns_pdm_geographic_risk') }}
)
select
    'PDM_UGANDA' as programme_id,
    current_date as as_of_date,
    loans.*,
    lifecycle.*,
    payments.payment_count,
    payments.unmatched_payment_count,
    payments.reconciled_payment_count / nullif(payments.payment_count, 0) as reconciliation_rate,
    loans.total_repaid_amount / nullif(loans.total_disbursed_amount, 0) as repayment_rate,
    lifecycle.total_credited_amount / nullif(lifecycle.total_instructed_amount, 0) as payment_success_rate,
    risks.high_risk_case_count,
    parishes.*,
    cast(null as double) as total_recovered_amount,
    'NOT_OBSERVABLE' as recovery_observability_status
from loans cross join lifecycle cross join payments cross join risks cross join parishes
