{{ config(materialized='iceberg_table') }}

with staging as (

    select
        event_id,
        loan_id,
        beneficiary_id,
        sacco_id,
        business_plan_id,
        application_date,
        approval_date,
        verification_date,
        disbursement_date,
        cashout_date,
        as_of_date,
        amount_requested,
        amount_approved,
        amount_disbursed,
        amount_repaid,
        principal_repaid,
        interest_rate,
        interest_charged,
        interest_paid,
        principal_outstanding_balance,
        interest_outstanding_balance,
        outstanding_balance,
        scheduled_instalment,
        repayment_rate,
        repayment_status,
        days_past_due,
        delinquency_bucket,
        loan_term_months,
        repayment_frequency,
        first_repayment_date,
        last_repayment_date,
        last_payment_date,
        loan_status,
        project_type,
        project_location,
        created_at,
        updated_at,
        kafka_topic,
        kafka_partition,
        kafka_offset,
        kafka_timestamp,
        category,
        source_system,
        source_group,
        year,
        month,
        day,
        load_timestamp,
        record_source
    from {{ ref('stg_pdm_pdmis_loans') }}

)

select *
from staging
