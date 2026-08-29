{{ config(materialized='iceberg_table') }}

-- Immutable payload audit with Kafka provenance.
select event_id, message_id, 'pain001' as event_type, 'vpm' as source_system,
       'icmn' as source_category, event_data, parsed_event_data,
       kafka_topic, kafka_partition, kafka_offset, kafka_timestamp
from {{ ref('br_pdm_icmn_vpm_pain001') }}

union all

select event_id, message_id, 'pain001', 'pmn', 'icmn', event_data, parsed_event_data,
       kafka_topic, kafka_partition, kafka_offset, kafka_timestamp
from {{ ref('br_pdm_icmn_pmn_pain001') }}

union all

select event_id, message_id, 'pain002', 'psn', 'cpo', event_data, parsed_event_data,
       kafka_topic, kafka_partition, kafka_offset, kafka_timestamp
from {{ ref('br_pdm_cpo_psn_pain002') }}

union all

select event_id, message_id, 'pain002', 'plm', 'cpo', event_data, parsed_event_data,
       kafka_topic, kafka_partition, kafka_offset, kafka_timestamp
from {{ ref('br_pdm_cpo_plm_pain002') }}
