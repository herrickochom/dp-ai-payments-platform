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
 where _kafka_topic = 'pdmis.business_plans'
)

select
    event_id,
    {{ extract_json('parsed_event_data', '$.business_plan_id') }} as business_plan_id,
    {{ extract_json('parsed_event_data', '$.loan_id') }} as loan_id,
    {{ extract_json('parsed_event_data', '$.beneficiary_id') }} as beneficiary_id,
    {{ extract_json('parsed_event_data', '$.project_name') }} as project_name,
    {{ extract_json('parsed_event_data', '$.project_type') }} as project_type,
    {{ extract_json('parsed_event_data', '$.description') }} as description,
    {{ extract_json('parsed_event_data', '$.location') }} as location,
    safe_cast({{ extract_json('parsed_event_data', '$.land_size') }} as numeric) as land_size,
    {{ extract_json('parsed_event_data', '$.market') }} as market,
    safe_cast({{ extract_json('parsed_event_data', '$.total_investment') }} as numeric) as total_investment,
    safe_cast({{ extract_json('parsed_event_data', '$.expected_revenue') }} as numeric) as expected_revenue,
    safe_cast({{ extract_json('parsed_event_data', '$.expected_costs') }} as numeric) as expected_costs,
    safe_cast({{ extract_json('parsed_event_data', '$.expected_profit') }} as numeric) as expected_profit,
    safe_cast({{ extract_json('parsed_event_data', '$.submission_date') }} as date) as submission_date,
    {{ extract_json('parsed_event_data', '$.approval_status') }} as approval_status,
    safe_cast({{ extract_json('parsed_event_data', '$.approval_date') }} as date) as approval_date,
    {{ extract_json('parsed_event_data', '$.approved_by') }} as approved_by,
    safe_cast({{ extract_json('parsed_event_data', '$.created_at') }} as timestamp) as created_at,
    safe_cast({{ extract_json('parsed_event_data', '$.updated_at') }} as timestamp) as updated_at,
    _kafka_metadata.topic as kafka_topic,
    _kafka_metadata.`partition` as kafka_partition,
    _kafka_metadata.offset as kafka_offset,
    safe_cast(_kafka_metadata.timestamp as timestamp) as kafka_timestamp,
    _kafka_metadata.category as category,
    'PDMIS' as source_system,
    'business_plans' as source_group,
    year, month, day,
    event_data, parsed_event_data,
    current_timestamp() as load_timestamp,
    'PDMIS_BUSINESS_PLANS' as record_source
from raw_data
where _kafka_metadata.topic = 'pdmis.business_plans'
  and parsed_event_data is not null
