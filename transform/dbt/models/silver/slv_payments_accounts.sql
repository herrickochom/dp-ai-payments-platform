{{ config(materialized='iceberg_table') }}

select event_id, message_id, 'vpm' as payment_source, 'debtor' as account_role,
       debtor_account_id as account_id, debtor_agent_id as agent_id,
       kafka_topic, kafka_partition, kafka_offset
from {{ ref('stg_icmn_vpm_pain001') }}

union all

select event_id, message_id, 'vpm' as payment_source, 'creditor' as account_role,
       creditor_account_id as account_id, creditor_agent_id as agent_id,
       kafka_topic, kafka_partition, kafka_offset
from {{ ref('stg_icmn_vpm_pain001') }}
