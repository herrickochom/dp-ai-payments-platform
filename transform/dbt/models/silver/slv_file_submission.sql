{{ config(materialized='iceberg_table') }}

with staged_submissions as (
    select kafka_topic, kafka_partition, kafka_offset, kafka_timestamp
    from {{ ref('stg_icmn_vpm_pain001') }}
    union all
    select kafka_topic, kafka_partition, kafka_offset, kafka_timestamp
    from {{ ref('stg_icmn_pmn_pain001') }}
    union all
    select kafka_topic, kafka_partition, kafka_offset, kafka_timestamp
    from {{ ref('stg_cpo_psn_pain002') }}
    union all
    select kafka_topic, kafka_partition, kafka_offset, kafka_timestamp
    from {{ ref('stg_cpo_plm_pain002') }}
)
select
    kafka_topic,
    kafka_partition,
    min(kafka_offset) as first_kafka_offset,
    max(kafka_offset) as last_kafka_offset,
    count(*) as event_count,
    min(kafka_timestamp) as first_received_at,
    max(kafka_timestamp) as last_received_at
from staged_submissions
group by kafka_topic, kafka_partition
