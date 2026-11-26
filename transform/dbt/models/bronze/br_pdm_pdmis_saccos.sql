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
 where _kafka_topic = 'pdmis.saccos'
)

select
    event_id,
    {{ extract_json('parsed_event_data', '$.sacco_id') }} as sacco_id,
    {{ extract_json('parsed_event_data', '$.name') }} as sacco_name,
    {{ extract_json('parsed_event_data', '$.registration_number') }} as registration_number,
    safe_cast({{ extract_json('parsed_event_data', '$.registration_date') }} as date) as registration_date,
    {{ extract_json('parsed_event_data', '$.wendi_account') }} as wendi_account,
    {{ extract_json('parsed_event_data', '$.postbank_account') }} as postbank_account,
    {{ extract_json('parsed_event_data', '$.chairperson') }} as chairperson,
    {{ extract_json('parsed_event_data', '$.secretary') }} as secretary,
    {{ extract_json('parsed_event_data', '$.treasurer') }} as treasurer,
    {{ extract_json('parsed_event_data', '$.office_address') }} as office_address,
    safe_cast({{ extract_json('parsed_event_data', '$.office_exists') }} as bool) as office_exists,
    {{ extract_json('parsed_event_data', '$.village') }} as village,
    {{ extract_json('parsed_event_data', '$.parish') }} as parish,
    {{ extract_json('parsed_event_data', '$.sub_county') }} as sub_county,
    {{ extract_json('parsed_event_data', '$.county') }} as county,
    {{ extract_json('parsed_event_data', '$.district') }} as district,
    {{ extract_json('parsed_event_data', '$.region') }} as region,
    safe_cast({{ extract_json('parsed_event_data', '$.number_of_beneficiaries') }} as int64) as number_of_beneficiaries,
    safe_cast({{ extract_json('parsed_event_data', '$.total_funds_received') }} as numeric) as total_funds_received,
    safe_cast({{ extract_json('parsed_event_data', '$.total_funds_disbursed') }} as numeric) as total_funds_disbursed,
    safe_cast({{ extract_json('parsed_event_data', '$.total_repayments') }} as numeric) as total_repayments,
    safe_cast({{ extract_json('parsed_event_data', '$.is_active') }} as bool) as is_active,
    safe_cast({{ extract_json('parsed_event_data', '$.created_at') }} as timestamp) as created_at,
    safe_cast({{ extract_json('parsed_event_data', '$.updated_at') }} as timestamp) as updated_at,
    _kafka_metadata.topic as kafka_topic,
    _kafka_metadata.`partition` as kafka_partition,
    _kafka_metadata.offset as kafka_offset,
    safe_cast(_kafka_metadata.timestamp as timestamp) as kafka_timestamp,
    _kafka_metadata.category as category,
    'PDMIS' as source_system,
    'saccos' as source_group,
    year, month, day,
    event_data, parsed_event_data,
    current_timestamp() as load_timestamp,
    'PDMIS_SACCOS' as record_source
from raw_data
where _kafka_metadata.topic = 'pdmis.saccos'
  and parsed_event_data is not null
