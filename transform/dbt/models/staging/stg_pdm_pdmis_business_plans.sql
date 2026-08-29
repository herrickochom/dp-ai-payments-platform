{{ config(materialized='iceberg_table') }}

with raw_data as (
    select event_id, event_data, parsed_event_data, _kafka_metadata, year, month, day
    from read_avro(
        's3://{{ var("s3_bucket") }}/{{ var("s3_path") }}/v2/**/topic=pdmis.business_plans/**/*.avro',
        hive_partitioning = true
    )
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
    try_cast({{ extract_json('parsed_event_data', '$.land_size') }} as decimal(18, 2)) as land_size,
    {{ extract_json('parsed_event_data', '$.market') }} as market,
    try_cast({{ extract_json('parsed_event_data', '$.total_investment') }} as decimal(18, 2)) as total_investment,
    try_cast({{ extract_json('parsed_event_data', '$.expected_revenue') }} as decimal(18, 2)) as expected_revenue,
    try_cast({{ extract_json('parsed_event_data', '$.expected_costs') }} as decimal(18, 2)) as expected_costs,
    try_cast({{ extract_json('parsed_event_data', '$.expected_profit') }} as decimal(18, 2)) as expected_profit,
    try_cast({{ extract_json('parsed_event_data', '$.submission_date') }} as date) as submission_date,
    {{ extract_json('parsed_event_data', '$.approval_status') }} as approval_status,
    try_cast({{ extract_json('parsed_event_data', '$.approval_date') }} as date) as approval_date,
    {{ extract_json('parsed_event_data', '$.approved_by') }} as approved_by,
    try_cast({{ extract_json('parsed_event_data', '$.created_at') }} as timestamp) as created_at,
    try_cast({{ extract_json('parsed_event_data', '$.updated_at') }} as timestamp) as updated_at,
    _kafka_metadata.topic as kafka_topic,
    _kafka_metadata.partition as kafka_partition,
    _kafka_metadata.offset as kafka_offset,
    try_cast(_kafka_metadata.timestamp as timestamp) as kafka_timestamp,
    _kafka_metadata.category as category,
    'PDMIS' as source_system,
    'business_plans' as source_group,
    year, month, day,
    event_data, parsed_event_data,
    current_timestamp as load_timestamp,
    'PDMIS_BUSINESS_PLANS' as record_source
from raw_data
where _kafka_metadata.topic = 'pdmis.business_plans'
  and parsed_event_data is not null
