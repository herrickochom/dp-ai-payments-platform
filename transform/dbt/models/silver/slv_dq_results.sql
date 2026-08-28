{{ config(materialized='iceberg_table') }}

select
    event_id,
    message_id,
    'vpm' as payment_source,
    kafka_topic,
    kafka_partition,
    kafka_offset,
    message_id is not null as message_id_valid,
    instructed_amount is not null and instructed_amount >= 0 as amount_valid,
    currency is not null and length(currency) = 3 as currency_valid,
    debtor_name is not null as debtor_valid,
    creditor_name is not null as creditor_valid,
    kafka_topic is not null
        and kafka_partition is not null
        and kafka_offset is not null as lineage_valid,
    case
        when message_id is not null
         and instructed_amount is not null and instructed_amount >= 0
         and currency is not null and length(currency) = 3
         and debtor_name is not null
         and creditor_name is not null
         and kafka_topic is not null
         and kafka_partition is not null
         and kafka_offset is not null
        then 'PASS'
        else 'FAIL'
    end as dq_status
from {{ ref('stg_icmn_vpm_pain001') }}
