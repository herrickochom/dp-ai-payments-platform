{{ config(materialized='iceberg_table') }}

with messages as (
    select event_id, message_id, 'pain.001' as message_type, 'ICMN_VPM' as source_system, creation_at,
           kafka_topic, kafka_partition, kafka_offset, kafka_timestamp from {{ ref('br_pdm_icmn_vpm_pain001') }}
    union all
    select event_id, message_id, 'pain.001', 'ICMN_PMN', creation_at,
           kafka_topic, kafka_partition, kafka_offset, kafka_timestamp from {{ ref('br_pdm_icmn_pmn_pain001') }}
    union all
    select event_id, message_id, 'pain.002', 'CPO_PSN', creation_at,
           kafka_topic, kafka_partition, kafka_offset, kafka_timestamp from {{ ref('br_pdm_cpo_psn_pain002') }}
    union all
    select event_id, message_id, 'pain.002', 'CPO_PLM', creation_at,
           kafka_topic, kafka_partition, kafka_offset, kafka_timestamp from {{ ref('br_pdm_cpo_plm_pain002') }}
    union all
    select event_id, message_id, 'pain.001', 'WENDI', creation_at,
           kafka_topic, kafka_partition, kafka_offset, kafka_timestamp from {{ ref('br_pdm_wendi_pain001') }}
    union all
    select event_id, message_id, 'pain.002', 'WENDI', creation_at,
           kafka_topic, kafka_partition, kafka_offset, kafka_timestamp from {{ ref('br_pdm_wendi_pain002') }}
    union all
    select event_id, message_id, 'pacs.008', 'MTN', creation_at,
           kafka_topic, kafka_partition, kafka_offset, kafka_timestamp from {{ ref('br_pdm_mobile_mtn_pacs008') }}
    union all
    select event_id, message_id, 'pacs.008', 'AIRTEL', creation_at,
           kafka_topic, kafka_partition, kafka_offset, kafka_timestamp from {{ ref('br_pdm_mobile_airtel_pacs008') }}
    union all
    select event_id, message_id, 'pacs.002', 'MTN', creation_at,
           kafka_topic, kafka_partition, kafka_offset, kafka_timestamp from {{ ref('br_pdm_mobile_mtn_pacs002') }}
    union all
    select event_id, message_id, 'pacs.002', 'AIRTEL', creation_at,
           kafka_topic, kafka_partition, kafka_offset, kafka_timestamp from {{ ref('br_pdm_mobile_airtel_pacs002') }}
)
select * from messages
qualify row_number() over (
    partition by source_system, message_id order by kafka_timestamp desc, kafka_offset desc
) = 1
