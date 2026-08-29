{{ config(materialized='view') }}

with raw_data as (
    select event_id, event_data, parsed_event_data, _kafka_metadata, year, month, day
    from read_avro(
        's3://{{ var("s3_bucket") }}/{{ var("s3_path") }}/v2/**/topic=pdmis.saccos/**/*.avro',
        hive_partitioning = true
    )
)

select
    event_id,
    {{ extract_json('parsed_event_data', '$.sacco_id') }} as sacco_id,
    {{ extract_json('parsed_event_data', '$.name') }} as sacco_name,
    {{ extract_json('parsed_event_data', '$.registration_number') }} as registration_number,
    try_cast({{ extract_json('parsed_event_data', '$.registration_date') }} as date) as registration_date,
    {{ extract_json('parsed_event_data', '$.wendi_account') }} as wendi_account,
    {{ extract_json('parsed_event_data', '$.postbank_account') }} as postbank_account,
    {{ extract_json('parsed_event_data', '$.chairperson') }} as chairperson,
    {{ extract_json('parsed_event_data', '$.secretary') }} as secretary,
    {{ extract_json('parsed_event_data', '$.treasurer') }} as treasurer,
    {{ extract_json('parsed_event_data', '$.office_address') }} as office_address,
    try_cast({{ extract_json('parsed_event_data', '$.office_exists') }} as boolean) as office_exists,
    {{ extract_json('parsed_event_data', '$.parish') }} as parish,
    {{ extract_json('parsed_event_data', '$.sub_county') }} as sub_county,
    {{ extract_json('parsed_event_data', '$.district') }} as district,
    {{ extract_json('parsed_event_data', '$.region') }} as region,
    try_cast({{ extract_json('parsed_event_data', '$.number_of_beneficiaries') }} as integer) as number_of_beneficiaries,
    try_cast({{ extract_json('parsed_event_data', '$.total_funds_received') }} as decimal(18, 2)) as total_funds_received,
    try_cast({{ extract_json('parsed_event_data', '$.total_funds_disbursed') }} as decimal(18, 2)) as total_funds_disbursed,
    try_cast({{ extract_json('parsed_event_data', '$.total_repayments') }} as decimal(18, 2)) as total_repayments,
    try_cast({{ extract_json('parsed_event_data', '$.is_active') }} as boolean) as is_active,
    try_cast({{ extract_json('parsed_event_data', '$.created_at') }} as timestamp) as created_at,
    try_cast({{ extract_json('parsed_event_data', '$.updated_at') }} as timestamp) as updated_at,
    _kafka_metadata.topic as kafka_topic,
    _kafka_metadata.partition as kafka_partition,
    _kafka_metadata.offset as kafka_offset,
    try_cast(_kafka_metadata.timestamp as timestamp) as kafka_timestamp,
    _kafka_metadata.category as category,
    'PDMIS' as source_system,
    'saccos' as source_group,
    year, month, day,
    event_data, parsed_event_data,
    current_timestamp as load_timestamp,
    'PDMIS_SACCOS' as record_source
from raw_data
where _kafka_metadata.topic = 'pdmis.saccos'
  and parsed_event_data is not null
