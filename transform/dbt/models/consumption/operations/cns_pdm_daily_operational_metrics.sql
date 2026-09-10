{{ config(materialized='iceberg_table', tags=['consumption', 'operations', 'daily']) }}

-- Grain: one row per reporting date. Loan portfolio measures use the source
-- as_of_date snapshot; disbursement measures use the actual disbursement date.
-- AI observation dates are not transaction dates, so AI counts are excluded.
with payment_daily as (
    select * from {{ ref('cns_pdm_payments_daily_summary') }}
), disbursement_daily as (
    select
        cast(disbursement_date as date) as reporting_date,
        sum(amount_disbursed) as disbursement_amount
    from {{ ref('gld_fct_pdm_loans') }}
    where disbursement_date is not null
    group by 1
), portfolio_daily as (
    select
        cast(as_of_date as date) as reporting_date,
        sum(amount_repaid) as repayment_amount,
        count(distinct beneficiary_sk) as beneficiary_count,
        count(*) as loan_count
    from {{ ref('gld_fct_pdm_loans') }}
    where as_of_date is not null
    group by 1
), lifecycle_daily as (
    select
        date.calendar_date as reporting_date,
        count(*) filter (
            where lifecycle.instructed_control_status = 'MISSING'
               or lifecycle.settled_control_status in ('MISSING', 'REJECTED')
               or lifecycle.credited_control_status = 'MISSING'
        ) as lifecycle_exception_count
    from {{ ref('gld_fct_pdm_payment_lifecycle') }} lifecycle
    left join {{ ref('gld_dim_pdm_date') }} date
      on lifecycle.approval_date_sk = date.date_sk
    group by 1
), reporting_dates as (
    select reporting_date from payment_daily
    union
    select reporting_date from disbursement_daily
    union
    select reporting_date from portfolio_daily
    union
    select reporting_date from lifecycle_daily
)
select
    dates.reporting_date,
    coalesce(payment.payment_count, 0) as payment_count,
    coalesce(payment.successful_payment_count, 0) as successful_payment_count,
    coalesce(payment.failed_payment_count, 0) as failed_payment_count,
    payment.payment_amount,
    disbursement.disbursement_amount,
    portfolio.repayment_amount,
    coalesce(portfolio.beneficiary_count, 0) as beneficiary_count,
    coalesce(portfolio.loan_count, 0) as loan_count,
    coalesce(payment.reconciled_payment_count, 0) as reconciled_payment_count,
    coalesce(payment.unreconciled_payment_count, 0) as unreconciled_payment_count,
    payment.reconciliation_rate,
    payment.payment_success_rate,
    coalesce(payment.operational_exception_count, 0)
        + coalesce(lifecycle.lifecycle_exception_count, 0) as operational_exception_count,
    cast(null as bigint) as ai_priority_review_count
from reporting_dates dates
left join payment_daily payment using (reporting_date)
left join disbursement_daily disbursement using (reporting_date)
left join portfolio_daily portfolio using (reporting_date)
left join lifecycle_daily lifecycle using (reporting_date)
where dates.reporting_date is not null
