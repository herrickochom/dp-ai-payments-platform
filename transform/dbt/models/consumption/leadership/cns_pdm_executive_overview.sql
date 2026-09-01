{{ config(materialized='iceberg_table', tags=['consumption', 'executive']) }}

with loan_base as (
    select
        loan.loan_id,
        loan.beneficiary_sk,
        loan.sacco_sk,
        loan.amount_approved,
        loan.amount_disbursed,
        loan.amount_repaid,
        loan.outstanding_amount,
        approval_date.calendar_date as approval_date,
        cast(date_trunc('month', approval_date.calendar_date) as date) as approval_month
    from {{ ref('gld_fct_pdm_loans') }} loan
    left join {{ ref('gld_dim_pdm_date') }} approval_date
      on loan.approval_date_sk = approval_date.date_sk

), loans as (
    select
        sum(amount_approved) as total_approved_amount,
        sum(amount_disbursed) as total_disbursed_amount,
        sum(amount_repaid) as total_repaid_amount,
        sum(outstanding_amount) as total_outstanding_amount,
        count(*) as loan_count,
        count(*) filter (where outstanding_amount > 0) as active_loan_count,
        count(distinct beneficiary_sk) as beneficiary_count,
        count(distinct sacco_sk) as sacco_count,
        max(approval_date) as latest_loan_date
    from loan_base

), lifecycle_by_loan as (
    select
        loan_id,
        sum(instructed_amount) as instructed_amount,
        sum(settled_amount) as settled_amount,
        sum(credited_amount) as credited_amount,
        sum(cashout_amount) as cashout_amount,
        count(*) filter (where intervention_priority <> 'LOW') as lifecycle_exception_count
    from {{ ref('cns_pdm_lifecycle_exceptions') }}
    group by 1

), lifecycle as (
    select
        sum(instructed_amount) as total_instructed_amount,
        sum(settled_amount) as total_settled_amount,
        sum(credited_amount) as total_credited_amount,
        sum(cashout_amount) as total_cashout_amount,
        sum(lifecycle_exception_count) as lifecycle_exception_count
    from lifecycle_by_loan

), payments as (
    select
        count(*) as payment_count,
        count(*) filter (where is_reconciled) as reconciled_payment_count,
        count(*) filter (where not is_entity_matched) as unmatched_payment_count,
        max(cast(occurred_at as date)) as latest_payment_date
    from {{ ref('gld_fct_pdm_payments') }}

), high_risk_beneficiaries as (
    select distinct beneficiary_sk
    from {{ ref('cns_pdm_beneficiary_identity_alerts') }}
    where identity_risk_band = 'HIGH'

), risks as (
    select count(*) as high_risk_case_count
    from high_risk_beneficiaries

), parishes as (
    select
        count(*) as reporting_parish_count,
        count(distinct district) as reporting_district_count,
        count(distinct region) as reporting_region_count,
        count(*) filter (where geographic_risk_band = 'LOW') as parishes_on_track,
        count(*) filter (where geographic_risk_band = 'MEDIUM') as parishes_at_risk,
        count(*) filter (where geographic_risk_band = 'HIGH') as parishes_critical,
        count(*) filter (where geographic_risk_band = 'SEVERE') as parishes_severe,
        count(*) filter (where geographic_risk_band = 'NO DATA') as parishes_no_data
    from {{ ref('cns_pdm_geographic_risk') }}

), monthly_kpis as (
    select
        loan.approval_month as reporting_month,

        sum(loan.amount_approved) as total_approved_amount,
        sum(coalesce(lifecycle.settled_amount, 0)) as total_settled_amount,
        sum(coalesce(lifecycle.credited_amount, 0)) as total_credited_amount,
        sum(loan.amount_disbursed) as total_disbursed_amount,

        -- This is the current outstanding state of loans in the approval cohort.
        -- It is not a reconstructed historical month-end balance.
        sum(loan.outstanding_amount) as total_outstanding_amount,

        count(distinct case
            when risk.beneficiary_sk is not null then loan.beneficiary_sk
        end) as high_risk_case_count

    from loan_base loan
    left join lifecycle_by_loan lifecycle
      on loan.loan_id = lifecycle.loan_id
    left join high_risk_beneficiaries risk
      on loan.beneficiary_sk = risk.beneficiary_sk
    where loan.approval_month is not null
    group by 1

), latest_kpi_month as (
    select max(reporting_month) as reporting_month
    from monthly_kpis

), current_month as (
    select
        monthly.reporting_month,
        monthly.total_approved_amount,
        monthly.total_settled_amount,
        monthly.total_credited_amount,
        monthly.total_disbursed_amount,
        monthly.total_outstanding_amount,
        monthly.high_risk_case_count
    from monthly_kpis monthly
    cross join latest_kpi_month latest
    where monthly.reporting_month = latest.reporting_month

), previous_month as (
    select
        monthly.reporting_month,
        monthly.total_approved_amount,
        monthly.total_settled_amount,
        monthly.total_credited_amount,
        monthly.total_disbursed_amount,
        monthly.total_outstanding_amount,
        monthly.high_risk_case_count
    from monthly_kpis monthly
    cross join latest_kpi_month latest
    where monthly.reporting_month = cast(latest.reporting_month - interval '1' month as date)

)

select
    'PDM_UGANDA' as programme_id,

    greatest(
        coalesce(loans.latest_loan_date, date '1900-01-01'),
        coalesce(payments.latest_payment_date, date '1900-01-01')
    ) as as_of_date,

    loans.*,
    lifecycle.*,

    payments.payment_count,
    payments.unmatched_payment_count,
    payments.reconciled_payment_count
        / nullif(payments.payment_count, 0) as reconciliation_rate,

    loans.total_repaid_amount
        / nullif(loans.total_disbursed_amount, 0) as repayment_rate,

    lifecycle.total_credited_amount
        / nullif(lifecycle.total_instructed_amount, 0) as payment_success_rate,

    risks.high_risk_case_count,

    parishes.*,

    current_month.reporting_month as kpi_reporting_month,
    cast(current_month.reporting_month - interval '1' month as date)
        as kpi_previous_month,

    'APPROVAL_COHORT_CURRENT_STATE' as mom_comparison_basis,

    -- TOTAL APPROVED
    current_month.total_approved_amount as current_month_total_approved_amount,
    previous_month.total_approved_amount as previous_month_total_approved_amount,
    {{ pdm_mom_pct_change(
        'current_month.total_approved_amount',
        'previous_month.total_approved_amount'
    ) }} as total_approved_amount_mom_pct,
    {{ pdm_mom_direction(
        'current_month.total_approved_amount',
        'previous_month.total_approved_amount'
    ) }} as total_approved_amount_mom_direction,
    {{ pdm_mom_arrow(
        'current_month.total_approved_amount',
        'previous_month.total_approved_amount'
    ) }} as total_approved_amount_mom_arrow,
    {{ pdm_mom_status(
        'current_month.total_approved_amount',
        'previous_month.total_approved_amount',
        increase_is_good=true
    ) }} as total_approved_amount_mom_status,

    -- TOTAL SETTLED
    current_month.total_settled_amount as current_month_total_settled_amount,
    previous_month.total_settled_amount as previous_month_total_settled_amount,
    {{ pdm_mom_pct_change(
        'current_month.total_settled_amount',
        'previous_month.total_settled_amount'
    ) }} as total_settled_amount_mom_pct,
    {{ pdm_mom_direction(
        'current_month.total_settled_amount',
        'previous_month.total_settled_amount'
    ) }} as total_settled_amount_mom_direction,
    {{ pdm_mom_arrow(
        'current_month.total_settled_amount',
        'previous_month.total_settled_amount'
    ) }} as total_settled_amount_mom_arrow,
    {{ pdm_mom_status(
        'current_month.total_settled_amount',
        'previous_month.total_settled_amount',
        increase_is_good=true
    ) }} as total_settled_amount_mom_status,

    -- TOTAL CREDITED
    current_month.total_credited_amount as current_month_total_credited_amount,
    previous_month.total_credited_amount as previous_month_total_credited_amount,
    {{ pdm_mom_pct_change(
        'current_month.total_credited_amount',
        'previous_month.total_credited_amount'
    ) }} as total_credited_amount_mom_pct,
    {{ pdm_mom_direction(
        'current_month.total_credited_amount',
        'previous_month.total_credited_amount'
    ) }} as total_credited_amount_mom_direction,
    {{ pdm_mom_arrow(
        'current_month.total_credited_amount',
        'previous_month.total_credited_amount'
    ) }} as total_credited_amount_mom_arrow,
    {{ pdm_mom_status(
        'current_month.total_credited_amount',
        'previous_month.total_credited_amount',
        increase_is_good=true
    ) }} as total_credited_amount_mom_status,

    -- TOTAL DISBURSED
    current_month.total_disbursed_amount as current_month_total_disbursed_amount,
    previous_month.total_disbursed_amount as previous_month_total_disbursed_amount,
    {{ pdm_mom_pct_change(
        'current_month.total_disbursed_amount',
        'previous_month.total_disbursed_amount'
    ) }} as total_disbursed_amount_mom_pct,
    {{ pdm_mom_direction(
        'current_month.total_disbursed_amount',
        'previous_month.total_disbursed_amount'
    ) }} as total_disbursed_amount_mom_direction,
    {{ pdm_mom_arrow(
        'current_month.total_disbursed_amount',
        'previous_month.total_disbursed_amount'
    ) }} as total_disbursed_amount_mom_arrow,
    {{ pdm_mom_status(
        'current_month.total_disbursed_amount',
        'previous_month.total_disbursed_amount',
        increase_is_good=true
    ) }} as total_disbursed_amount_mom_status,

    -- OUTSTANDING
    current_month.total_outstanding_amount as current_month_total_outstanding_amount,
    previous_month.total_outstanding_amount as previous_month_total_outstanding_amount,
    {{ pdm_mom_pct_change(
        'current_month.total_outstanding_amount',
        'previous_month.total_outstanding_amount'
    ) }} as total_outstanding_amount_mom_pct,
    {{ pdm_mom_direction(
        'current_month.total_outstanding_amount',
        'previous_month.total_outstanding_amount'
    ) }} as total_outstanding_amount_mom_direction,
    {{ pdm_mom_arrow(
        'current_month.total_outstanding_amount',
        'previous_month.total_outstanding_amount'
    ) }} as total_outstanding_amount_mom_arrow,
    {{ pdm_mom_status(
        'current_month.total_outstanding_amount',
        'previous_month.total_outstanding_amount',
        increase_is_good=false
    ) }} as total_outstanding_amount_mom_status,

    -- HIGH RISK CASES
    current_month.high_risk_case_count as current_month_high_risk_case_count,
    previous_month.high_risk_case_count as previous_month_high_risk_case_count,
    {{ pdm_mom_pct_change(
        'current_month.high_risk_case_count',
        'previous_month.high_risk_case_count'
    ) }} as high_risk_case_count_mom_pct,
    {{ pdm_mom_direction(
        'current_month.high_risk_case_count',
        'previous_month.high_risk_case_count'
    ) }} as high_risk_case_count_mom_direction,
    {{ pdm_mom_arrow(
        'current_month.high_risk_case_count',
        'previous_month.high_risk_case_count'
    ) }} as high_risk_case_count_mom_arrow,
    {{ pdm_mom_status(
        'current_month.high_risk_case_count',
        'previous_month.high_risk_case_count',
        increase_is_good=false
    ) }} as high_risk_case_count_mom_status,

    'LIMITED_SAMPLE' as reporting_scope_status,

    cast(null as double) as total_recovered_amount,
    'NOT_OBSERVABLE' as recovery_observability_status

from loans
cross join lifecycle
cross join payments
cross join risks
cross join parishes
left join current_month on true
left join previous_month on true
