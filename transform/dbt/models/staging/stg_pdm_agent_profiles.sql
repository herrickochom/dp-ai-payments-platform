{{ config(materialized='view') }}

with raw_data as (
    select event_id, event_data, parsed_event_data, _kafka_metadata, year, month, day
    from read_avro(
        's3://{{ var("s3_bucket") }}/{{ var("s3_path") }}/v2/**/topic=agent.profiles/**/*.avro',
        hive_partitioning = true
    )
)

select
    event_id,
    {{ extract_json('parsed_event_data', '$.agent_id') }} as agent_id,
    {{ extract_json('parsed_event_data', '$.agent_code') }} as agent_code,
    {{ extract_json('parsed_event_data', '$.name') }} as agent_name,
    {{ extract_json('parsed_event_data', '$.phone') }} as phone,
    {{ extract_json('parsed_event_data', '$.registration_number') }} as registration_number,
    try_cast({{ extract_json('parsed_event_data', '$.registration_date') }} as date) as registration_date,
    {{ extract_json('parsed_event_data', '$.network_provider') }} as network_provider,
    try_cast({{ extract_json('parsed_event_data', '$.commission_rate') }} as decimal(9, 4)) as commission_rate,
    {{ extract_json('parsed_event_data', '$.location') }} as location,
    {{ extract_json('parsed_event_data', '$.parish') }} as parish,
    {{ extract_json('parsed_event_data', '$.district') }} as district,
    {{ extract_json('parsed_event_data', '$.region') }} as region,
    try_cast({{ extract_json('parsed_event_data', '$.verified') }} as boolean) as verified,
    try_cast({{ extract_json('parsed_event_data', '$.is_active') }} as boolean) as is_active,
    try_cast({{ extract_json('parsed_event_data', '$.created_at') }} as timestamp) as created_at,
    try_cast({{ extract_json('parsed_event_data', '$.updated_at') }} as timestamp) as updated_at,
    _kafka_metadata.topic as kafka_topic,
    _kafka_metadata.partition as kafka_partition,
    _kafka_metadata.offset as kafka_offset,
    try_cast(_kafka_metadata.timestamp as timestamp) as kafka_timestamp,
    year, month, day, event_data, parsed_event_data,
    current_timestamp as load_timestamp,
    'AGENT_PROFILES' as record_source
from raw_data
where _kafka_metadata.topic = 'agent.profiles'
  and parsed_event_data is not null
