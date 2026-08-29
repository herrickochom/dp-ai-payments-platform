{{ config(materialized='iceberg_table') }}

with transactions as (
    select transaction_id, end_to_end_id, instruction_id, uetr, message_id,
           cast(null as varchar) as beneficiary_id, cast(null as varchar) as sacco_id,
           cast(null as varchar) as agent_id,
           'ICMN_VPM' as source_system, creation_at as occurred_at,
           instructed_amount as amount, currency, cast(null as varchar) as transaction_status
    from {{ ref('br_pdm_icmn_vpm_pain001') }}
    union all
    select transaction_id, end_to_end_id, instruction_id, uetr, message_id, null, null, null,
           'WENDI_PAIN001', creation_at, instructed_amount, currency, null
    from {{ ref('br_pdm_wendi_pain001') }}
    union all
    select transaction_id, end_to_end_id, null, null, message_id, null, null, null,
           'MTN_PACS008', creation_at, instructed_amount, currency, null
    from {{ ref('br_pdm_mobile_mtn_pacs008') }}
    union all
    select transaction_id, end_to_end_id, null, null, message_id, null, null, null,
           'AIRTEL_PACS008', creation_at, instructed_amount, currency, null
    from {{ ref('br_pdm_mobile_airtel_pacs008') }}
    union all
    select coalesce(wendi_transaction_id, wallet_event_id), loan_id, null, null, null,
           beneficiary_id, sacco_id, agent_id,
           'WENDI_WALLET', event_timestamp, amount, currency, transaction_status
    from {{ ref('br_pdm_wendi_transactions') }}
    union all
    select coalesce(transaction_id, source_json_transaction_id), coalesce(loan_id, source_json_loan_id),
           null, null, null, coalesce(beneficiary_id, source_json_beneficiary_id), null,
           coalesce(agent_id, source_json_agent_id), 'AGENT',
           coalesce(transaction_timestamp, try_cast(source_json_transaction_timestamp as timestamp)),
           coalesce(amount, try_cast(source_json_amount as double)), 'UGX', coalesce(status, source_json_status)
    from {{ ref('br_pdm_agent_transactions') }}
)
select * from transactions
where transaction_id is not null
qualify row_number() over (
    partition by source_system, transaction_id order by occurred_at desc nulls last
) = 1
