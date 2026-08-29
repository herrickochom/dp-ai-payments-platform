{{ config(materialized='iceberg_table') }}

with raw_data as (
    select event_id, event_data, parsed_event_data, _kafka_metadata, year, month, day
    from read_avro(
        's3://{{ var("s3_bucket") }}/{{ var("s3_path") }}/v2/**/topic=agent.locations/**/*.avro',
        hive_partitioning = true
    )
)

select
    event_id,
    {{ extract_json('parsed_event_data', '$.agent_id') }} as agent_id,
    {{ extract_json('parsed_event_data', '$.location_type') }} as location_type,
    {{ extract_json('parsed_event_data', '$.address') }} as address,
    try_cast({{ extract_json('parsed_event_data', '$.latitude') }} as double) as latitude,
    try_cast({{ extract_json('parsed_event_data', '$.longitude') }} as double) as longitude,
    try_cast({{ extract_json('parsed_event_data', '$.is_active') }} as boolean) as is_active,
    _kafka_metadata.topic as kafka_topic,
    _kafka_metadata.partition as kafka_partition,
    _kafka_metadata.offset as kafka_offset,
    try_cast(_kafka_metadata.timestamp as timestamp) as kafka_timestamp,
    year, month, day, event_data, parsed_event_data,
    current_timestamp as load_timestamp,
    'AGENT_LOCATIONS' as record_source
from raw_data
where _kafka_metadata.topic = 'agent.locations'
  and parsed_event_data is not null
