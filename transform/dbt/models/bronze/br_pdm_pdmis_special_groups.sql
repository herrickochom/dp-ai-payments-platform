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
 where _kafka_topic = 'pdmis.special_groups'
)

select
    event_id,
    {{ extract_json('parsed_event_data', '$.group_code') }} as group_code,
    {{ extract_json('parsed_event_data', '$.group_name') }} as group_name,
    {{ extract_json('parsed_event_data', '$.description') }} as description,
    safe_cast({{ extract_json('parsed_event_data', '$.quota_percentage') }} as numeric)
        as quota_percentage,
    _kafka_metadata.topic as kafka_topic,
    _kafka_metadata.`partition` as kafka_partition,
    _kafka_metadata.offset as kafka_offset,
    safe_cast(_kafka_metadata.timestamp as timestamp) as kafka_timestamp,
    _kafka_metadata.category as category,
    'PDMIS' as source_system,
    'special_groups' as source_group,
    year, month, day,
    event_data, parsed_event_data,
    current_timestamp() as load_timestamp,
    'PDMIS_SPECIAL_GROUPS' as record_source
from raw_data
where _kafka_metadata.topic = 'pdmis.special_groups'
  and parsed_event_data is not null
