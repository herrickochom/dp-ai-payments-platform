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
 where _kafka_topic = 'pdmis.beneficiaries'
)

select
    event_id,
    {{ extract_json('parsed_event_data', '$.beneficiary_id') }} as beneficiary_id,
    {{ extract_json('parsed_event_data', '$.beneficiary_token') }} as beneficiary_token,
    {{ extract_json('parsed_event_data', '$.nin') }} as nin,
    {{ extract_json('parsed_event_data', '$.nin_hashed') }} as nin_hashed,
    safe_cast({{ extract_json('parsed_event_data', '$.nin_verified') }} as bool) as nin_verified,
    {{ extract_json('parsed_event_data', '$.name') }} as beneficiary_name,
    cast(
        SAFE.PARSE_DATE('%d/%m/%Y', {{ extract_json('parsed_event_data', '$.date_of_birth') }}) as date
    ) as date_of_birth,
    {{ extract_json('parsed_event_data', '$.gender') }} as gender,
    {{ extract_json('parsed_event_data', '$.phone') }} as phone,
    {{ extract_json('parsed_event_data', '$.alternative_phone') }} as alternative_phone,
    {{ extract_json('parsed_event_data', '$.email') }} as email,
    safe_cast({{ extract_json('parsed_event_data', '$.phone_verified') }} as bool) as phone_verified,
    {{ extract_json('parsed_event_data', '$.household_id') }} as household_id,
    {{ extract_json('parsed_event_data', '$.special_group_code') }} as special_group_code,
    {{ extract_json('parsed_event_data', '$.special_group') }} as special_group,
    {{ extract_json('parsed_event_data', '$.village') }} as village,
    {{ extract_json('parsed_event_data', '$.parish') }} as parish,
    {{ extract_json('parsed_event_data', '$.sub_county') }} as sub_county,
    {{ extract_json('parsed_event_data', '$.county') }} as county,
    {{ extract_json('parsed_event_data', '$.district') }} as district,
    {{ extract_json('parsed_event_data', '$.region') }} as region,
    safe_cast({{ extract_json('parsed_event_data', '$.registration_date') }} as date) as registration_date,
    safe_cast({{ extract_json('parsed_event_data', '$.is_active') }} as bool) as is_active,
    safe_cast({{ extract_json('parsed_event_data', '$.created_at') }} as timestamp) as created_at,
    safe_cast({{ extract_json('parsed_event_data', '$.updated_at') }} as timestamp) as updated_at,
    _kafka_metadata.topic as kafka_topic,
    _kafka_metadata.`partition` as kafka_partition,
    _kafka_metadata.offset as kafka_offset,
    safe_cast(_kafka_metadata.timestamp as timestamp) as kafka_timestamp,
    _kafka_metadata.category as category,
    'PDMIS' as source_system,
    'beneficiaries' as source_group,
    year, month, day,
    event_data, parsed_event_data,
    current_timestamp() as load_timestamp,
    'PDMIS_BENEFICIARIES' as record_source
from raw_data
where _kafka_metadata.topic = 'pdmis.beneficiaries'
  and parsed_event_data is not null
