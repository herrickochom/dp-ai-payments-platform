{{ config(materialized='view') }}

with raw_data as (
    select event_id, event_data, parsed_event_data, _kafka_metadata, year, month, day
    from read_avro(
        's3://{{ var("s3_bucket") }}/{{ var("s3_path") }}/v2/**/topic=pdmis.loans/**/*.avro',
        hive_partitioning = true
    )
)

select
    event_id,
    {{ extract_json('parsed_event_data', '$.loan_id') }} as loan_id,
    {{ extract_json('parsed_event_data', '$.beneficiary_id') }} as beneficiary_id,
    {{ extract_json('parsed_event_data', '$.sacco_id') }} as sacco_id,
    {{ extract_json('parsed_event_data', '$.business_plan_id') }} as business_plan_id,
    try_cast({{ extract_json('parsed_event_data', '$.application_date') }} as date) as application_date,
    try_cast({{ extract_json('parsed_event_data', '$.approval_date') }} as date) as approval_date,
    try_cast({{ extract_json('parsed_event_data', '$.amount_requested') }} as decimal(18, 2)) as amount_requested,
    try_cast({{ extract_json('parsed_event_data', '$.amount_approved') }} as decimal(18, 2)) as amount_approved,
    try_cast({{ extract_json('parsed_event_data', '$.amount_disbursed') }} as decimal(18, 2)) as amount_disbursed,
    try_cast({{ extract_json('parsed_event_data', '$.amount_repaid') }} as decimal(18, 2)) as amount_repaid,
    try_cast({{ extract_json('parsed_event_data', '$.interest_rate') }} as decimal(9, 4)) as interest_rate,
    try_cast({{ extract_json('parsed_event_data', '$.interest_charged') }} as decimal(18, 2)) as interest_charged,
    try_cast({{ extract_json('parsed_event_data', '$.interest_paid') }} as decimal(18, 2)) as interest_paid,
    try_cast({{ extract_json('parsed_event_data', '$.loan_term_months') }} as integer) as loan_term_months,
    {{ extract_json('parsed_event_data', '$.repayment_frequency') }} as repayment_frequency,
    try_cast({{ extract_json('parsed_event_data', '$.first_repayment_date') }} as date) as first_repayment_date,
    try_cast({{ extract_json('parsed_event_data', '$.last_repayment_date') }} as date) as last_repayment_date,
    {{ extract_json('parsed_event_data', '$.loan_status') }} as loan_status,
    {{ extract_json('parsed_event_data', '$.project_type') }} as project_type,
    {{ extract_json('parsed_event_data', '$.project_location') }} as project_location,
    try_cast({{ extract_json('parsed_event_data', '$.created_at') }} as timestamp) as created_at,
    try_cast({{ extract_json('parsed_event_data', '$.updated_at') }} as timestamp) as updated_at,
    _kafka_metadata.topic as kafka_topic,
    _kafka_metadata.partition as kafka_partition,
    _kafka_metadata.offset as kafka_offset,
    try_cast(_kafka_metadata.timestamp as timestamp) as kafka_timestamp,
    _kafka_metadata.category as category,
    'PDMIS' as source_system,
    'loans' as source_group,
    year, month, day,
    event_data, parsed_event_data,
    current_timestamp as load_timestamp,
    'PDMIS_LOANS' as record_source
from raw_data
where _kafka_metadata.topic = 'pdmis.loans'
  and parsed_event_data is not null
