{{ config(materialized='iceberg_table') }}

with reconciliation as (
    select transaction_source as source_system, transaction_id,
        count(*) as status_event_count,
        count(*) filter (where reconciliation_status = 'MATCHED') as matched_status_event_count,
        count(*) filter (where reconciliation_status = 'UNMATCHED') as unmatched_status_event_count
    from {{ ref('slv_pdm_payments_reconciliation') }}
    where transaction_id is not null
    group by 1, 2
), creditors as (
    select message_id, account_id, account_issuer
    from {{ ref('slv_pdm_payment_party_roles') }}
    where party_role = 'CREDITOR'
)
select
    {{ gold_surrogate_key(['payment.source_system', 'payment.transaction_id']) }} as payment_sk,
    {{ gold_surrogate_key(['cast(payment.occurred_at as date)']) }} as payment_date_sk,
    {{ gold_surrogate_key(['coalesce(entity.transaction_beneficiary_id, entity.loan_beneficiary_id)']) }} as beneficiary_sk,
    {{ gold_surrogate_key(['coalesce(entity.transaction_sacco_id, entity.loan_sacco_id)']) }} as sacco_sk,
    {{ gold_surrogate_key(['agent.agent_id']) }} as agent_sk,
    {{ gold_surrogate_key(['beneficiary.region', 'beneficiary.district', 'beneficiary.parish', 'beneficiary.village']) }} as geography_sk,
    payment.source_system, payment.transaction_id, payment.end_to_end_id as loan_id,
    payment.message_id, payment.instruction_id, payment.uetr, payment.occurred_at,
    payment.currency, payment.transaction_status, entity.entity_match_status,
    sha256(coalesce(creditors.account_id, '__UNKNOWN__')) as creditor_account_hashed,
    creditors.account_issuer as creditor_account_issuer,
    coalesce(reconciliation.status_event_count, 0) as status_event_count,
    coalesce(reconciliation.matched_status_event_count, 0) as matched_status_event_count,
    coalesce(reconciliation.unmatched_status_event_count, 0) as unmatched_status_event_count,
    payment.amount as payment_amount,
    1 as payment_count,
    entity.entity_match_status = 'MATCHED' as is_entity_matched,
    coalesce(reconciliation.matched_status_event_count, 0) > 0
        and coalesce(reconciliation.unmatched_status_event_count, 0) = 0 as is_reconciled,
    creditors.account_id is not null and beneficiary.phone is not null
        and creditors.account_id <> beneficiary.phone as is_account_substituted
from {{ ref('slv_pdm_payments_transactions') }} payment
left join {{ ref('slv_pdm_payment_entity_matches') }} entity
  on payment.source_system = entity.source_system and payment.transaction_id = entity.transaction_id
left join {{ ref('slv_pdm_beneficiaries') }} beneficiary
  on coalesce(entity.transaction_beneficiary_id, entity.loan_beneficiary_id) = beneficiary.beneficiary_id
left join {{ ref('slv_pdm_agents') }} agent
  on entity.transaction_agent_id = agent.agent_id
left join reconciliation
  on payment.source_system = reconciliation.source_system and payment.transaction_id = reconciliation.transaction_id
left join creditors on payment.message_id = creditors.message_id
