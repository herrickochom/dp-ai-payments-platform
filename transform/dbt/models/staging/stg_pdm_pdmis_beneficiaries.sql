{{ config(materialized='view') }}

with raw_data as (
    select event_id, event_data, parsed_event_data, _kafka_metadata, year, month, day
    from read_avro(
        's3://{{ var("s3_bucket") }}/{{ var("s3_path") }}/v2/**/topic=pdmis.beneficiaries/**/*.avro',
        hive_partitioning = true
    )
)

select
    event_id,
    {{ extract_json('parsed_event_data', '$.beneficiary_id') }} as beneficiary_id,
    {{ extract_json('parsed_event_data', '$.beneficiary_token') }} as beneficiary_token,
    {{ extract_json('parsed_event_data', '$.nin') }} as nin,
    {{ extract_json('parsed_event_data', '$.nin_hashed') }} as nin_hashed,
    try_cast({{ extract_json('parsed_event_data', '$.nin_verified') }} as boolean) as nin_verified,
    {{ extract_json('parsed_event_data', '$.name') }} as beneficiary_name,
    try_cast({{ extract_json('parsed_event_data', '$.date_of_birth') }} as date) as date_of_birth,
    {{ extract_json('parsed_event_data', '$.gender') }} as gender,
    {{ extract_json('parsed_event_data', '$.phone') }} as phone,
    {{ extract_json('parsed_event_data', '$.alternative_phone') }} as alternative_phone,
    {{ extract_json('parsed_event_data', '$.email') }} as email,
    try_cast({{ extract_json('parsed_event_data', '$.phone_verified') }} as boolean) as phone_verified,
    {{ extract_json('parsed_event_data', '$.household_id') }} as household_id,
    {{ extract_json('parsed_event_data', '$.special_group_code') }} as special_group_code,
    {{ extract_json('parsed_event_data', '$.village') }} as village,
    {{ extract_json('parsed_event_data', '$.parish') }} as parish,
    {{ extract_json('parsed_event_data', '$.district') }} as district,
    {{ extract_json('parsed_event_data', '$.region') }} as region,
    try_cast({{ extract_json('parsed_event_data', '$.registration_date') }} as date) as registration_date,
    try_cast({{ extract_json('parsed_event_data', '$.is_active') }} as boolean) as is_active,
    try_cast({{ extract_json('parsed_event_data', '$.created_at') }} as timestamp) as created_at,
    try_cast({{ extract_json('parsed_event_data', '$.updated_at') }} as timestamp) as updated_at,
    _kafka_metadata.topic as kafka_topic,
    _kafka_metadata.partition as kafka_partition,
    _kafka_metadata.offset as kafka_offset,
    try_cast(_kafka_metadata.timestamp as timestamp) as kafka_timestamp,
    _kafka_metadata.category as category,
    'PDMIS' as source_system,
    'beneficiaries' as source_group,
    year, month, day,
    event_data, parsed_event_data,
    current_timestamp as load_timestamp,
    'PDMIS_BENEFICIARIES' as record_source
from raw_data
where _kafka_metadata.topic = 'pdmis.beneficiaries'
  and parsed_event_data is not null
