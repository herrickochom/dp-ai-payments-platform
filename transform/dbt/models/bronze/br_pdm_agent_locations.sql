{{ config(materialized='pdm_iceberg_guarded') }}

with raw_data as (
 select *, struct(_kafka_topic as topic, _kafka_partition as `partition`,
 _kafka_offset as offset, _kafka_timestamp as timestamp,
 _kafka_category as category, _kafka_source_group as source_group,
 _kafka_source_system as source_system) as _kafka_metadata,
 extract(year from _kafka_timestamp) as year,
 extract(month from _kafka_timestamp) as month,
 extract(day from _kafka_timestamp) as day
 from {{ source('pdm_bronze_input','payment_events') }}
 where _kafka_topic = 'agent.locations'
)

select
    event_id,
    {{ extract_json('parsed_event_data', '$.agent_id') }} as agent_id,
    {{ extract_json('parsed_event_data', '$.location_type') }} as location_type,
    {{ extract_json('parsed_event_data', '$.address') }} as address,
    safe_cast({{ extract_json('parsed_event_data', '$.latitude') }} as float64) as latitude,
    safe_cast({{ extract_json('parsed_event_data', '$.longitude') }} as float64) as longitude,
    safe_cast({{ extract_json('parsed_event_data', '$.is_active') }} as bool) as is_active,
    _kafka_metadata.topic as kafka_topic,
    _kafka_metadata.`partition` as kafka_partition,
    _kafka_metadata.offset as kafka_offset,
    safe_cast(_kafka_metadata.timestamp as timestamp) as kafka_timestamp,
    year, month, day, event_data, parsed_event_data,
    current_timestamp() as load_timestamp,
    'AGENT_LOCATIONS' as record_source
from raw_data
where _kafka_metadata.topic = 'agent.locations'
  and parsed_event_data is not null
