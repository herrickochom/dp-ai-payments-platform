{{ config(materialized='iceberg_table', tags=['gold', 'privacy-boundary']) }}

-- GATE 2 PRIVACY BOUNDARY: atomic loan fact keyed to the canonical
-- beneficiary_token via the pseudonymous loan linkage. Projects only approved
-- coarse geography (region/district/county/sub_county) carried from Silver.

select
    {{ gold_surrogate_key(['loan.loan_id']) }} as loan_sk,
    {{ gold_surrogate_key(['loan_link.beneficiary_token']) }} as beneficiary_sk,
    {{ gold_surrogate_key(['loan.sacco_id']) }} as sacco_sk,
    {{ gold_surrogate_key(['beneficiary.region', 'beneficiary.district', 'beneficiary.county', 'beneficiary.sub_county']) }} as geography_sk,
    {{ gold_surrogate_key(['cast(loan.application_date as date)']) }} as application_date_sk,
    {{ gold_surrogate_key(['cast(loan.approval_date as date)']) }} as approval_date_sk,
    loan.loan_id, loan.business_plan_id, loan.loan_status, loan.project_type,
    loan.repayment_frequency, loan.loan_term_months, loan.amount_requested,
    loan.amount_approved, loan.amount_disbursed, loan.amount_repaid,
    loan.principal_repaid,
    loan.interest_rate, loan.interest_charged, loan.interest_paid,
    loan.principal_outstanding_balance, loan.interest_outstanding_balance,
    loan.outstanding_balance,
    -- Backward-compatible alias. This is total contractual outstanding, not principal-only.
    loan.outstanding_balance as outstanding_amount,
    loan.scheduled_instalment, loan.repayment_rate, loan.repayment_status,
    loan.days_past_due, loan.delinquency_bucket,
    loan.disbursement_date, loan.cashout_date, loan.as_of_date, loan.last_payment_date,
    coalesce(loan.amount_approved, 0) - coalesce(loan.amount_disbursed, 0) as undisbursed_amount,
    1 as loan_count,
    loan_link.beneficiary_token,
    beneficiary.region,
    beneficiary.district,
    beneficiary.county,
    beneficiary.sub_county
from {{ ref('slv_pdm_loans') }} loan
left join {{ ref('slv_pdm_loan_beneficiary_links') }} loan_link
    on loan.loan_id = loan_link.loan_id
left join {{ ref('slv_pdm_beneficiaries') }} beneficiary
    on loan_link.beneficiary_token = beneficiary.beneficiary_token
