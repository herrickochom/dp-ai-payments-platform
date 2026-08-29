{{ config(materialized='iceberg_table') }}

with raw_data as (
    select event_id, event_data, parsed_event_data, _kafka_metadata, year, month, day
    from read_avro(
        's3://{{ var("s3_bucket") }}/{{ var("s3_path") }}/v2/**/topic=pdmis.households/**/*.avro',
        hive_partitioning = true
    )
)

select
    event_id,
    {{ extract_json('parsed_event_data', '$.household_id') }} as household_id,
    {{ extract_json('parsed_event_data', '$.head_of_household') }} as head_of_household,
    {{ extract_json('parsed_event_data', '$.head_phone') }} as head_phone,
    try_cast({{ extract_json('parsed_event_data', '$.member_count') }} as integer) as member_count,
    try_cast({{ extract_json('parsed_event_data', '$.adults_count') }} as integer) as adults_count,
    try_cast({{ extract_json('parsed_event_data', '$.children_count') }} as integer) as children_count,
    {{ extract_json('parsed_event_data', '$.economic_status') }} as economic_status,
    {{ extract_json('parsed_event_data', '$.food_security_status') }} as food_security_status,
    {{ extract_json('parsed_event_data', '$.housing_type') }} as housing_type,
    {{ extract_json('parsed_event_data', '$.land_ownership') }} as land_ownership,
    {{ extract_json('parsed_event_data', '$.village') }} as village,
    {{ extract_json('parsed_event_data', '$.parish') }} as parish,
    {{ extract_json('parsed_event_data', '$.district') }} as district,
    {{ extract_json('parsed_event_data', '$.region') }} as region,
    try_cast({{ extract_json('parsed_event_data', '$.registration_date') }} as date) as registration_date,
    try_cast({{ extract_json('parsed_event_data', '$.created_at') }} as timestamp) as created_at,
    try_cast({{ extract_json('parsed_event_data', '$.updated_at') }} as timestamp) as updated_at,
    _kafka_metadata.topic as kafka_topic,
    _kafka_metadata.partition as kafka_partition,
    _kafka_metadata.offset as kafka_offset,
    try_cast(_kafka_metadata.timestamp as timestamp) as kafka_timestamp,
    _kafka_metadata.category as category,
    'PDMIS' as source_system,
    'households' as source_group,
    year, month, day,
    event_data, parsed_event_data,
    current_timestamp as load_timestamp,
    'PDMIS_HOUSEHOLDS' as record_source
from raw_data
where _kafka_metadata.topic = 'pdmis.households'
  and parsed_event_data is not null
