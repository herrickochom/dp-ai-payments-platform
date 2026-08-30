{{ config(materialized='iceberg_table') }}

select
    payment.payment_sk as cashout_sk, payment.payment_date_sk as cashout_date_sk,
    payment.beneficiary_sk, payment.agent_sk, payment.geography_sk,
    payment.transaction_id, payment.loan_id, payment.occurred_at as cashout_at,
    payment.currency, payment.transaction_status, payment.payment_amount as cashout_amount,
    payment.is_entity_matched, payment.is_reconciled,
    agent.verified as agent_verified, agent.is_active as agent_active,
    case when loan.approval_date is null or payment.occurred_at is null then null
        else date_diff('day', cast(loan.approval_date as date), cast(payment.occurred_at as date))
    end as days_from_loan_approval,
    payment.payment_amount > coalesce(loan.amount_approved, loan.amount_disbursed) as exceeds_approved_amount,
    1 as cashout_count
from {{ ref('gld_fct_pdm_payments') }} payment
left join {{ ref('slv_pdm_loans') }} loan on payment.loan_id = loan.loan_id
left join {{ ref('slv_pdm_agents') }} agent
  on payment.agent_sk = {{ gold_surrogate_key(['agent.agent_id']) }}
where payment.source_system = 'AGENT'
