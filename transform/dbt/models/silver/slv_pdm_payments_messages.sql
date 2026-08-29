{{ config(materialized='iceberg_table') }}

select event_id, message_id, 'pain001' as event_type, 'vpm' as source_system,
       'icmn' as source_category, creation_at,
       kafka_topic, kafka_partition, kafka_offset, kafka_timestamp
from {{ ref('br_pdm_icmn_vpm_pain001') }}

union all

select event_id, message_id, 'pain001', 'pmn', 'icmn', creation_at,
       kafka_topic, kafka_partition, kafka_offset, kafka_timestamp
from {{ ref('br_pdm_icmn_pmn_pain001') }}

union all

select event_id, message_id, 'pain002', 'psn', 'cpo', creation_at,
       kafka_topic, kafka_partition, kafka_offset, kafka_timestamp
from {{ ref('br_pdm_cpo_psn_pain002') }}

union all

select event_id, message_id, 'pain002', 'plm', 'cpo', creation_at,
       kafka_topic, kafka_partition, kafka_offset, kafka_timestamp
from {{ ref('br_pdm_cpo_plm_pain002') }}
