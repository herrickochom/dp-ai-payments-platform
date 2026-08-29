{{ config(materialized='iceberg_table') }}

select
    loan_id, beneficiary_id, sacco_id, business_plan_id, application_date,
    approval_date, amount_requested, amount_approved, amount_disbursed,
    amount_repaid, interest_rate, interest_charged, interest_paid,
    loan_term_months, repayment_frequency, first_repayment_date,
    last_repayment_date, loan_status, project_type, project_location,
    created_at, updated_at
from {{ ref('br_pdm_pdmis_loans') }}
qualify row_number() over (
    partition by loan_id order by updated_at desc nulls last, kafka_timestamp desc, kafka_offset desc
) = 1
