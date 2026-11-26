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
 where _kafka_topic = 'agent.profiles'
)

select
    event_id,
    {{ extract_json('parsed_event_data', '$.agent_id') }} as agent_id,
    {{ extract_json('parsed_event_data', '$.agent_code') }} as agent_code,
    {{ extract_json('parsed_event_data', '$.name') }} as agent_name,
    {{ extract_json('parsed_event_data', '$.phone') }} as phone,
    {{ extract_json('parsed_event_data', '$.registration_number') }} as registration_number,
    safe_cast({{ extract_json('parsed_event_data', '$.registration_date') }} as date) as registration_date,
    {{ extract_json('parsed_event_data', '$.network_provider') }} as network_provider,
    safe_cast({{ extract_json('parsed_event_data', '$.commission_rate') }} as numeric) as commission_rate,
    {{ extract_json('parsed_event_data', '$.location') }} as location,
    {{ extract_json('parsed_event_data', '$.country') }} as country,
    {{ extract_json('parsed_event_data', '$.village') }} as village,
    {{ extract_json('parsed_event_data', '$.parish') }} as parish,
    {{ extract_json('parsed_event_data', '$.sub_county') }} as sub_county,
    {{ extract_json('parsed_event_data', '$.county') }} as county,
    {{ extract_json('parsed_event_data', '$.district') }} as district,
    {{ extract_json('parsed_event_data', '$.region') }} as region,
    safe_cast({{ extract_json('parsed_event_data', '$.verified') }} as bool) as verified,
    safe_cast({{ extract_json('parsed_event_data', '$.is_active') }} as bool) as is_active,
    safe_cast({{ extract_json('parsed_event_data', '$.created_at') }} as timestamp) as created_at,
    safe_cast({{ extract_json('parsed_event_data', '$.updated_at') }} as timestamp) as updated_at,
    _kafka_metadata.topic as kafka_topic,
    _kafka_metadata.`partition` as kafka_partition,
    _kafka_metadata.offset as kafka_offset,
    safe_cast(_kafka_metadata.timestamp as timestamp) as kafka_timestamp,
    year, month, day, event_data, parsed_event_data,
    current_timestamp() as load_timestamp,
    'AGENT_PROFILES' as record_source
from raw_data
where _kafka_metadata.topic = 'agent.profiles'
  and parsed_event_data is not null
