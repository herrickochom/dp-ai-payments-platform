{{ config(materialized='iceberg_table') }}

with transactions as (

    select
        transaction_id,
        end_to_end_id,
        instruction_id,
        uetr,
        message_id,
        cast(null as varchar) as beneficiary_id,
        cast(null as varchar) as sacco_id,
        cast(null as varchar) as agent_id,
        'PAYMENT_INSTRUCTION' as payment_type,
        'ICMN_VPM' as source_system,
        creation_at as occurred_at,
        instructed_amount as amount,
        currency,
        cast(null as varchar) as transaction_status
    from {{ source('bronze', 'br_pdm_icmn_vpm_pain001') }}

    union all

    select
        transaction_id,
        end_to_end_id,
        instruction_id,
        uetr,
        message_id,
        cast(null as varchar) as beneficiary_id,
        cast(null as varchar) as sacco_id,
        cast(null as varchar) as agent_id,
        'PAYMENT_ROUTING' as payment_type,
        'WENDI_PAIN001' as source_system,
        creation_at as occurred_at,
        instructed_amount as amount,
        currency,
        cast(null as varchar) as transaction_status
    from {{ source('bronze', 'br_pdm_wendi_pain001') }}

    union all

    select
        transaction_id,
        end_to_end_id,
        cast(null as varchar) as instruction_id,
        cast(null as varchar) as uetr,
        message_id,
        cast(null as varchar) as beneficiary_id,
        cast(null as varchar) as sacco_id,
        cast(null as varchar) as agent_id,
        'SETTLEMENT' as payment_type,
        'MTN_PACS008' as source_system,
        creation_at as occurred_at,
        instructed_amount as amount,
        currency,
        cast(null as varchar) as transaction_status
    from {{ source('bronze', 'br_pdm_mobile_mtn_pacs008') }}

    union all

    select
        transaction_id,
        end_to_end_id,
        cast(null as varchar) as instruction_id,
        cast(null as varchar) as uetr,
        message_id,
        cast(null as varchar) as beneficiary_id,
        cast(null as varchar) as sacco_id,
        cast(null as varchar) as agent_id,
        'SETTLEMENT' as payment_type,
        'AIRTEL_PACS008' as source_system,
        creation_at as occurred_at,
        instructed_amount as amount,
        currency,
        cast(null as varchar) as transaction_status
    from {{ source('bronze', 'br_pdm_mobile_airtel_pacs008') }}

    union all

    select
        coalesce(wendi_transaction_id, wallet_event_id) as transaction_id,
        loan_id as end_to_end_id,
        cast(null as varchar) as instruction_id,
        cast(null as varchar) as uetr,
        cast(null as varchar) as message_id,
        beneficiary_id,
        sacco_id,
        agent_id,
        'WALLET_TRANSACTION' as payment_type,
        'WENDI_WALLET' as source_system,
        event_timestamp as occurred_at,
        amount,
        currency,
        transaction_status
    from {{ source('bronze', 'br_pdm_wendi_transactions') }}

    union all

    select
        transaction_id,
        loan_id as end_to_end_id,
        cast(null as varchar) as instruction_id,
        cast(null as varchar) as uetr,
        cast(null as varchar) as message_id,
        beneficiary_id,
        cast(null as varchar) as sacco_id,
        agent_id,
        'AGENT_CASHOUT' as payment_type,
        'AGENT' as source_system,
        transaction_timestamp as occurred_at,
        amount,
        'UGX' as currency,
        status as transaction_status
    from {{ source('bronze', 'br_pdm_agent_transactions') }}

),

identified_transactions as (

    select
        *,
        coalesce(
            transaction_id,
            instruction_id,
            end_to_end_id,
            message_id
        ) as record_identifier
    from transactions

)

select *
from identified_transactions
where record_identifier is not null
qualify row_number() over (
    partition by source_system, payment_type, record_identifier
    order by occurred_at desc nulls last
) = 1
