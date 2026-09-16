{{ config(materialized='iceberg_table', tags=['gold', 'privacy-boundary']) }}

-- GATE 2 PRIVACY BOUNDARY: atomic accumulating payment fact keyed to the
-- canonical beneficiary_token via the pseudonymous loan linkage. Legitimate
-- PAYMENT TECHNICAL identifiers (transaction/message/instruction/UETR, entity
-- match and reconciliation states) are preserved unchanged. The plain SHA-256
-- creditor-account hashing is retired from ordinary Gold: the account
-- substitution control is computed at the Silver boundary and consumed as a
-- boolean, and only the account issuer is carried.

with reconciliation as (
    select transaction_source as source_system, transaction_id,
        count(*) as status_event_count,
        count(*) filter (where reconciliation_status = 'MATCHED') as matched_status_event_count,
        count(*) filter (where reconciliation_status = 'UNMATCHED') as unmatched_status_event_count
    from {{ ref('slv_pdm_payments_reconciliation') }}
    where transaction_id is not null
    group by 1, 2
),
controls as (
    select source_system, transaction_id, creditor_account_issuer, is_account_substituted
    from {{ ref('slv_pdm_payment_account_controls') }}
)
select
    {{ gold_surrogate_key(['payment.source_system', 'payment.transaction_id']) }} as payment_sk,
    {{ gold_surrogate_key(['cast(payment.occurred_at as date)']) }} as payment_date_sk,
    {{ gold_surrogate_key(['loan_link.beneficiary_token']) }} as beneficiary_sk,
    {{ gold_surrogate_key(['coalesce(entity.transaction_sacco_id, entity.loan_sacco_id)']) }} as sacco_sk,
    {{ gold_surrogate_key(['agent.agent_id']) }} as agent_sk,
    {{ gold_surrogate_key(['beneficiary.region', 'beneficiary.district', 'beneficiary.county', 'beneficiary.sub_county']) }} as geography_sk,
    payment.source_system, payment.transaction_id, payment.end_to_end_id as loan_id,
    payment.message_id, payment.instruction_id, payment.uetr, payment.occurred_at,
    payment.currency, payment.transaction_status, entity.entity_match_status,
    loan_link.beneficiary_token,
    beneficiary.region,
    beneficiary.district,
    beneficiary.county,
    beneficiary.sub_county,
    controls.creditor_account_issuer,
    coalesce(reconciliation.status_event_count, 0) as status_event_count,
    coalesce(reconciliation.matched_status_event_count, 0) as matched_status_event_count,
    coalesce(reconciliation.unmatched_status_event_count, 0) as unmatched_status_event_count,
    payment.amount as payment_amount,
    1 as payment_count,
    entity.entity_match_status = 'MATCHED' as is_entity_matched,
    coalesce(reconciliation.matched_status_event_count, 0) > 0
        and coalesce(reconciliation.unmatched_status_event_count, 0) = 0 as is_reconciled,
    controls.is_account_substituted
from {{ ref('slv_pdm_payments_transactions') }} payment
left join {{ ref('slv_pdm_payment_entity_matches') }} entity
  on payment.source_system = entity.source_system and payment.transaction_id = entity.transaction_id
left join {{ ref('slv_pdm_loan_beneficiary_links') }} loan_link
  on payment.end_to_end_id = loan_link.loan_id
left join {{ ref('slv_pdm_beneficiaries') }} beneficiary
  on loan_link.beneficiary_token = beneficiary.beneficiary_token
left join {{ ref('slv_pdm_agents') }} agent
  on entity.transaction_agent_id = agent.agent_id
left join reconciliation
  on payment.source_system = reconciliation.source_system and payment.transaction_id = reconciliation.transaction_id
left join controls
  on payment.source_system = controls.source_system and payment.transaction_id = controls.transaction_id
