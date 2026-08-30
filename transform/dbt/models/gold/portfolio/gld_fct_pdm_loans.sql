{{ config(materialized='iceberg_table') }}

select
    {{ gold_surrogate_key(['loan.loan_id']) }} as loan_sk,
    {{ gold_surrogate_key(['loan.beneficiary_id']) }} as beneficiary_sk,
    {{ gold_surrogate_key(['loan.sacco_id']) }} as sacco_sk,
    {{ gold_surrogate_key(['beneficiary.region', 'beneficiary.district', 'beneficiary.parish', 'beneficiary.village']) }} as geography_sk,
    {{ gold_surrogate_key(['cast(loan.application_date as date)']) }} as application_date_sk,
    {{ gold_surrogate_key(['cast(loan.approval_date as date)']) }} as approval_date_sk,
    loan.loan_id, loan.business_plan_id, loan.loan_status, loan.project_type,
    loan.repayment_frequency, loan.loan_term_months, loan.amount_requested,
    loan.amount_approved, loan.amount_disbursed, loan.amount_repaid,
    loan.interest_rate, loan.interest_charged, loan.interest_paid,
    greatest(coalesce(loan.amount_disbursed, 0) + coalesce(loan.interest_charged, 0)
        - coalesce(loan.amount_repaid, 0), 0) as outstanding_amount,
    coalesce(loan.amount_approved, 0) - coalesce(loan.amount_disbursed, 0) as undisbursed_amount,
    1 as loan_count
from {{ ref('slv_pdm_loans') }} loan
left join {{ ref('slv_pdm_beneficiaries') }} beneficiary on loan.beneficiary_id = beneficiary.beneficiary_id
