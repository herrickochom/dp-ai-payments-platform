{{ config(materialized='iceberg_table') }}

with candidates as (
    select
        status.status_source,
        status.message_id as status_message_id,
        status.original_message_id,
        message.source_system as original_message_source,
        message.message_id as payment_message_id,
        status.original_transaction_id,
        status.end_to_end_id,
        coalesce(exact_payment.source_system, message_payment.source_system,
            fallback_payment.source_system) as transaction_source,
        coalesce(exact_payment.transaction_id, message_payment.transaction_id,
            fallback_payment.transaction_id) as transaction_id,
        case
            when message.message_id is not null
              or exact_payment.transaction_id is not null
              or message_payment.transaction_id is not null
              or fallback_payment.transaction_id is not null then 'MATCHED'
            else 'UNMATCHED'
        end as reconciliation_status,
        case
            when exact_payment.transaction_id is not null then 'TRANSACTION_ID'
            when message_payment.transaction_id is not null then 'MESSAGE_TRANSACTION'
            when message.message_id is not null then 'MESSAGE_ID'
            when fallback_payment.transaction_id is not null then 'END_TO_END_ID_AND_SOURCE'
            else null
        end as match_method,
        row_number() over (
            partition by status.status_source, status.message_id
            order by
                case
                    when exact_payment.transaction_id is not null then 1
                    when message_payment.transaction_id is not null then 2
                    when message.message_id is not null then 3
                    else 4
                end,
                fallback_payment.transaction_id
        ) as candidate_rank
    from {{ ref('slv_pdm_payments_status_report') }} status
    left join {{ ref('slv_pdm_payments_messages') }} message
      on status.original_message_id = message.message_id
    left join {{ ref('slv_pdm_payments_transactions') }} exact_payment
      on status.original_transaction_id = exact_payment.transaction_id
    left join {{ ref('slv_pdm_payments_transactions') }} message_payment
      on message.source_system = message_payment.source_system
     and message.message_id = message_payment.message_id
    left join {{ ref('slv_pdm_payments_transactions') }} fallback_payment
      on status.end_to_end_id = fallback_payment.end_to_end_id
     and fallback_payment.source_system = case status.status_source
            when 'CPO_PSN' then 'ICMN_VPM'
            when 'WENDI' then 'WENDI_PAIN001'
            when 'MTN' then 'MTN_PACS008'
            when 'AIRTEL' then 'AIRTEL_PACS008'
         end
)
select
    status_source, status_message_id, original_message_id,
    original_message_source, payment_message_id, original_transaction_id,
    end_to_end_id, transaction_source, transaction_id,
    reconciliation_status, match_method
from candidates
where candidate_rank = 1
