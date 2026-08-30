{{ config(materialized='iceberg_table', tags=['consumption', 'audit'], meta={'classification': 'restricted'}) }}

with payment_keys as (
    select
        loan_id,
        max(transaction_id) filter (where source_system in ('ICMN_VPM', 'WENDI_PAIN001')) as pain001_transaction_id,
        max(message_id) filter (where source_system in ('ICMN_VPM', 'WENDI_PAIN001')) as pain001_message_id,
        max(transaction_id) filter (where source_system in ('MTN_PACS008', 'AIRTEL_PACS008')) as pacs008_transaction_id,
        max(transaction_id) filter (where source_system = 'WENDI_WALLET') as wallet_transaction_id,
        max(transaction_id) filter (where source_system = 'AGENT') as agent_transaction_id,
        max(instruction_id) as instruction_id,
        max(uetr) as uetr,
        count(*) filter (where not is_entity_matched) as entity_mismatch_count,
        count(*) filter (where not is_reconciled) as reconciliation_exception_count
    from {{ ref('gld_fct_pdm_payments') }}
    where loan_id is not null
    group by 1
)
select
    lifecycle.lifecycle_sk,
    lifecycle.loan_id,
    loan.business_plan_id,
    beneficiary.beneficiary_id,
    sacco.sacco_id,
    keys.pain001_message_id,
    keys.pain001_transaction_id,
    keys.pacs008_transaction_id,
    keys.wallet_transaction_id,
    keys.agent_transaction_id,
    keys.instruction_id,
    keys.uetr,
    cast(null as varchar) as pain002_message_id,
    cast(null as varchar) as pacs002_status_id,
    cast(null as varchar) as camt_reference,
    lifecycle.amount_approved as approved_amount,
    lifecycle.instructed_amount,
    lifecycle.settled_amount,
    lifecycle.credited_amount,
    lifecycle.cashout_amount,
    lifecycle.amount_repaid as repaid_amount,
    coalesce(keys.entity_mismatch_count, 0) as entity_mismatch_count,
    coalesce(keys.reconciliation_exception_count, 0) as reconciliation_exception_count,
    lifecycle.intervention_priority as lifecycle_status,
    case when coalesce(keys.entity_mismatch_count, 0) = 0 then 'MATCHED' else 'EXCEPTION' end as entity_match_status,
    case when coalesce(keys.reconciliation_exception_count, 0) = 0 then 'MATCHED' else 'EXCEPTION' end as reconciliation_status,
    'NOT_OBSERVABLE_IN_GOLD' as status_identifier_observability
from {{ ref('cns_pdm_lifecycle_exceptions') }} lifecycle
left join {{ ref('gld_fct_pdm_loans') }} loan using (loan_id)
left join {{ ref('gld_dim_pdm_beneficiary') }} beneficiary
  on lifecycle.beneficiary_sk = beneficiary.beneficiary_sk
left join {{ ref('gld_dim_pdm_sacco') }} sacco
  on lifecycle.sacco_sk = sacco.sacco_sk
left join payment_keys keys on lifecycle.loan_id = keys.loan_id
