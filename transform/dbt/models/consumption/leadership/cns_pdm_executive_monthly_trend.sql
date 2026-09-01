{{ config(materialized='iceberg_table', tags=['consumption', 'executive', 'trend']) }}

-- Loan amounts are attributed to the month in which the loan was approved.
-- They are cohort measures, not reconstructed month-end balance snapshots.
with loan_cohorts as (
    select
        date_trunc('month', approval_date.calendar_date) as reporting_month,
        count(distinct loan.loan_id) as approved_loan_count,
        count(distinct loan.beneficiary_sk) as approved_beneficiary_count,
        sum(loan.amount_approved) as approved_amount,
        sum(loan.amount_disbursed) as disbursed_amount,
        sum(loan.amount_repaid) as repaid_amount,
        sum(loan.outstanding_amount) as current_outstanding_amount,
        sum(loan.amount_disbursed)
            / nullif(sum(loan.amount_approved), 0) as cohort_disbursement_rate,
        sum(loan.amount_repaid)
            / nullif(sum(loan.amount_disbursed), 0) as cohort_repayment_rate
    from {{ ref('gld_fct_pdm_loans') }} loan
    inner join {{ ref('gld_dim_pdm_date') }} approval_date
      on loan.approval_date_sk = approval_date.date_sk
    where approval_date.calendar_date is not null
    group by 1
),

-- Payment measures use the actual payment occurrence date and therefore
-- represent genuine monthly transaction activity.
payment_activity as (
    select
        date_trunc('month', payment_date.calendar_date) as reporting_month,
        count(distinct payment.transaction_id) as payment_count,
        sum(payment.payment_amount) as payment_amount,
        count(distinct payment.transaction_id) filter (
            where payment.is_reconciled
        ) as reconciled_payment_count,
        count(distinct payment.transaction_id) filter (
            where payment.is_entity_matched
        ) as entity_matched_payment_count,
        count(distinct payment.transaction_id) filter (
            where payment.is_account_substituted
        ) as account_substitution_count
    from {{ ref('gld_fct_pdm_payments') }} payment
    inner join {{ ref('gld_dim_pdm_date') }} payment_date
      on payment.payment_date_sk = payment_date.date_sk
    where payment_date.calendar_date is not null
    group by 1
),

combined as (
    select
        coalesce(loan.reporting_month, payment.reporting_month) as reporting_month,
        loan.approved_loan_count,
        loan.approved_beneficiary_count,
        loan.approved_amount,
        loan.disbursed_amount,
        loan.repaid_amount,
        loan.current_outstanding_amount,
        loan.cohort_disbursement_rate,
        loan.cohort_repayment_rate,
        payment.payment_count,
        payment.payment_amount,
        payment.reconciled_payment_count,
        payment.entity_matched_payment_count,
        payment.account_substitution_count
    from loan_cohorts loan
    full outer join payment_activity payment using (reporting_month)
)

select
    cast(reporting_month as date) as reporting_month,
    year(reporting_month) as reporting_year,
    quarter(reporting_month) as reporting_quarter,
    month(reporting_month) as reporting_month_number,
    coalesce(approved_loan_count, 0) as approved_loan_count,
    coalesce(approved_beneficiary_count, 0) as approved_beneficiary_count,
    coalesce(approved_amount, 0) as approved_amount,
    coalesce(disbursed_amount, 0) as disbursed_amount,
    coalesce(repaid_amount, 0) as repaid_amount,
    coalesce(current_outstanding_amount, 0) as current_outstanding_amount,
    coalesce(cohort_disbursement_rate, 0) as cohort_disbursement_rate,
    coalesce(cohort_repayment_rate, 0) as cohort_repayment_rate,
    coalesce(payment_count, 0) as payment_count,
    coalesce(payment_amount, 0) as payment_amount,
    coalesce(reconciled_payment_count, 0) as reconciled_payment_count,
    coalesce(entity_matched_payment_count, 0) as entity_matched_payment_count,
    coalesce(account_substitution_count, 0) as account_substitution_count,
    coalesce(reconciled_payment_count, 0)
        / nullif(coalesce(payment_count, 0), 0) as payment_reconciliation_rate,
    coalesce(entity_matched_payment_count, 0)
        / nullif(coalesce(payment_count, 0), 0) as payment_entity_match_rate
from combined
