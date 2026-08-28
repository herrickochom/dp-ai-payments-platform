{{ config(materialized='iceberg_table') }}

select
    event_id,
    {{ extract_json('parsed_event_data', '$.header.message_id') }} as message_id,
    try_cast({{ extract_json('parsed_event_data', '$.header.creation_date') }} as timestamp) as creation_at,
    {{ extract_json('parsed_event_data', '$.header.initiating_party') }} as initiating_party,
    {{ extract_json('parsed_event_data', '$.header.original_message_id') }} as original_message_id,
    {{ extract_json('parsed_event_data', '$.header.original_message_type') }} as original_message_type,
    {{ extract_json('parsed_event_data', '$.header.group_status') }} as group_status,
    {{ extract_json('parsed_event_data', '$.payload.transaction_status') }} as transaction_status,
    {{ extract_json('parsed_event_data', '$.payload.reason_code') }} as reason_code,
    {{ extract_json('parsed_event_data', '$.payload.additional_info') }} as additional_info,
    {{ extract_json('parsed_event_data', '$.payload.settlement_status') }} as settlement_status,
    try_cast({{ extract_json('parsed_event_data', '$.payload.business_date') }} as date) as business_date,
    event_data,
    parsed_event_data,
    _kafka_metadata.topic as kafka_topic,
    _kafka_metadata.partition as kafka_partition,
    _kafka_metadata.offset as kafka_offset,
    _kafka_metadata.timestamp as kafka_timestamp
from {{ bronze_valid_events() }}
where _kafka_metadata.topic = 'cpo.psn.pain002'
  and parsed_event_data is not null
