{{ config(materialized='iceberg_table') }}

select distinct
    payment.source_system,
    payment.transaction_id,
    payment.end_to_end_id,
    payment.beneficiary_id as transaction_beneficiary_id,
    payment.sacco_id as transaction_sacco_id,
    payment.agent_id as transaction_agent_id,
    loan.loan_id,
    loan.beneficiary_id as loan_beneficiary_id,
    loan.sacco_id as loan_sacco_id,
    loan.business_plan_id,
    beneficiary.beneficiary_id as matched_beneficiary_id,
    sacco.sacco_id as matched_sacco_id,
    business_plan.business_plan_id as matched_business_plan_id,
    agent.agent_id as matched_agent_id,
    case
        when payment.end_to_end_id is null then 'NOT_APPLICABLE'
        when loan.loan_id is null then 'LOAN_NOT_MATCHED'
        when loan.beneficiary_id is not null and beneficiary.beneficiary_id is null then 'BENEFICIARY_NOT_MATCHED'
        when payment.beneficiary_id is not null and payment.beneficiary_id <> loan.beneficiary_id then 'BENEFICIARY_MISMATCH'
        when loan.sacco_id is not null and sacco.sacco_id is null then 'SACCO_NOT_MATCHED'
        when payment.sacco_id is not null and payment.sacco_id <> loan.sacco_id then 'SACCO_MISMATCH'
        when loan.business_plan_id is not null and business_plan.business_plan_id is null then 'BUSINESS_PLAN_NOT_MATCHED'
        when payment.agent_id is not null and agent.agent_id is null then 'AGENT_NOT_MATCHED'
        else 'MATCHED'
    end as entity_match_status
from {{ ref('slv_pdm_payments_transactions') }} payment
left join {{ ref('slv_pdm_loans') }} loan on payment.end_to_end_id = loan.loan_id
left join {{ ref('slv_pdm_beneficiaries') }} beneficiary on loan.beneficiary_id = beneficiary.beneficiary_id
left join {{ ref('slv_pdm_saccos') }} sacco on loan.sacco_id = sacco.sacco_id
left join {{ ref('slv_pdm_business_plans') }} business_plan on loan.business_plan_id = business_plan.business_plan_id
left join {{ ref('slv_pdm_agents') }} agent on payment.agent_id = agent.agent_id
