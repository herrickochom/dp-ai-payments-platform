{{ config(materialized='iceberg_table') }}

with raw_data as (
    select event_id, event_data, parsed_event_data, _kafka_metadata, year, month, day
    from read_avro(
        's3://{{ var("s3_bucket") }}/{{ var("s3_path") }}/v2/**/topic=pdmis.special_groups/**/*.avro',
        hive_partitioning = true
    )
)

select
    event_id,
    {{ extract_json('parsed_event_data', '$.group_code') }} as group_code,
    {{ extract_json('parsed_event_data', '$.group_name') }} as group_name,
    {{ extract_json('parsed_event_data', '$.description') }} as description,
    try_cast({{ extract_json('parsed_event_data', '$.quota_percentage') }} as decimal(5, 2))
        as quota_percentage,
    _kafka_metadata.topic as kafka_topic,
    _kafka_metadata.partition as kafka_partition,
    _kafka_metadata.offset as kafka_offset,
    try_cast(_kafka_metadata.timestamp as timestamp) as kafka_timestamp,
    _kafka_metadata.category as category,
    'PDMIS' as source_system,
    'special_groups' as source_group,
    year, month, day,
    event_data, parsed_event_data,
    current_timestamp as load_timestamp,
    'PDMIS_SPECIAL_GROUPS' as record_source
from raw_data
where _kafka_metadata.topic = 'pdmis.special_groups'
  and parsed_event_data is not null
