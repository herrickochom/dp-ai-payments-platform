{{ config(materialized='iceberg_table') }}

select distinct
    status.status_source,
    status.message_id as status_message_id,
    status.original_message_id,
    message.source_system as original_message_source,
    message.message_id as payment_message_id,
    status.original_transaction_id,
    status.end_to_end_id,
    payment.source_system as transaction_source,
    payment.transaction_id,
    case
        when message.message_id is not null or payment.transaction_id is not null then 'MATCHED'
        else 'UNMATCHED'
    end as reconciliation_status,
    case
        when status.original_transaction_id is not null
         and payment.transaction_id = status.original_transaction_id then 'TRANSACTION_ID'
        when status.original_message_id is not null
         and message.message_id = status.original_message_id then 'MESSAGE_ID'
        when status.end_to_end_id is not null
         and payment.end_to_end_id = status.end_to_end_id then 'END_TO_END_ID'
        else null
    end as match_method
from {{ ref('slv_pdm_payments_status_report') }} status
left join {{ ref('slv_pdm_payments_messages') }} message
  on status.original_message_id = message.message_id
left join {{ ref('slv_pdm_payments_transactions') }} payment
  on status.original_transaction_id = payment.transaction_id
  or (
      status.original_transaction_id is null
      and status.end_to_end_id = payment.end_to_end_id
  )
