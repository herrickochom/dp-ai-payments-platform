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
 where _kafka_topic = 'pdmis.households'
)

select
    event_id,
    {{ extract_json('parsed_event_data', '$.household_id') }} as household_id,
    {{ extract_json('parsed_event_data', '$.head_of_household') }} as head_of_household,
    {{ extract_json('parsed_event_data', '$.head_phone') }} as head_phone,
    safe_cast({{ extract_json('parsed_event_data', '$.member_count') }} as int64) as member_count,
    safe_cast({{ extract_json('parsed_event_data', '$.adults_count') }} as int64) as adults_count,
    safe_cast({{ extract_json('parsed_event_data', '$.children_count') }} as int64) as children_count,
    {{ extract_json('parsed_event_data', '$.economic_status') }} as economic_status,
    {{ extract_json('parsed_event_data', '$.food_security_status') }} as food_security_status,
    {{ extract_json('parsed_event_data', '$.housing_type') }} as housing_type,
    {{ extract_json('parsed_event_data', '$.land_ownership') }} as land_ownership,
    {{ extract_json('parsed_event_data', '$.village') }} as village,
    {{ extract_json('parsed_event_data', '$.parish') }} as parish,
    {{ extract_json('parsed_event_data', '$.sub_county') }} as sub_county,
    {{ extract_json('parsed_event_data', '$.county') }} as county,
    {{ extract_json('parsed_event_data', '$.district') }} as district,
    {{ extract_json('parsed_event_data', '$.region') }} as region,
    safe_cast({{ extract_json('parsed_event_data', '$.registration_date') }} as date) as registration_date,
    safe_cast({{ extract_json('parsed_event_data', '$.created_at') }} as timestamp) as created_at,
    safe_cast({{ extract_json('parsed_event_data', '$.updated_at') }} as timestamp) as updated_at,
    _kafka_metadata.topic as kafka_topic,
    _kafka_metadata.`partition` as kafka_partition,
    _kafka_metadata.offset as kafka_offset,
    safe_cast(_kafka_metadata.timestamp as timestamp) as kafka_timestamp,
    _kafka_metadata.category as category,
    'PDMIS' as source_system,
    'households' as source_group,
    year, month, day,
    event_data, parsed_event_data,
    current_timestamp() as load_timestamp,
    'PDMIS_HOUSEHOLDS' as record_source
from raw_data
where _kafka_metadata.topic = 'pdmis.households'
  and parsed_event_data is not null
