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
 where _kafka_topic = 'pdmis.loans'
)

select
    event_id,
    {{ extract_json('parsed_event_data', '$.loan_id') }} as loan_id,
    {{ extract_json('parsed_event_data', '$.beneficiary_id') }} as beneficiary_id,
    {{ extract_json('parsed_event_data', '$.sacco_id') }} as sacco_id,
    {{ extract_json('parsed_event_data', '$.business_plan_id') }} as business_plan_id,
    safe_cast({{ extract_json('parsed_event_data', '$.application_date') }} as date) as application_date,
    safe_cast({{ extract_json('parsed_event_data', '$.approval_date') }} as date) as approval_date,
    safe_cast({{ extract_json('parsed_event_data', '$.verification_date') }} as date) as verification_date,
    safe_cast({{ extract_json('parsed_event_data', '$.disbursement_date') }} as date) as disbursement_date,
    safe_cast({{ extract_json('parsed_event_data', '$.cashout_date') }} as date) as cashout_date,
    safe_cast({{ extract_json('parsed_event_data', '$.as_of_date') }} as date) as as_of_date,
    safe_cast({{ extract_json('parsed_event_data', '$.amount_requested') }} as numeric) as amount_requested,
    safe_cast({{ extract_json('parsed_event_data', '$.amount_approved') }} as numeric) as amount_approved,
    safe_cast({{ extract_json('parsed_event_data', '$.amount_disbursed') }} as numeric) as amount_disbursed,
    safe_cast({{ extract_json('parsed_event_data', '$.amount_repaid') }} as numeric) as amount_repaid,
    safe_cast({{ extract_json('parsed_event_data', '$.principal_repaid') }} as numeric) as principal_repaid,
    safe_cast({{ extract_json('parsed_event_data', '$.interest_rate') }} as numeric) as interest_rate,
    safe_cast({{ extract_json('parsed_event_data', '$.interest_charged') }} as numeric) as interest_charged,
    safe_cast({{ extract_json('parsed_event_data', '$.interest_paid') }} as numeric) as interest_paid,
    safe_cast({{ extract_json('parsed_event_data', '$.principal_outstanding_balance') }} as numeric) as principal_outstanding_balance,
    safe_cast({{ extract_json('parsed_event_data', '$.interest_outstanding_balance') }} as numeric) as interest_outstanding_balance,
    safe_cast({{ extract_json('parsed_event_data', '$.outstanding_balance') }} as numeric) as outstanding_balance,
    safe_cast({{ extract_json('parsed_event_data', '$.scheduled_instalment') }} as numeric) as scheduled_instalment,
    safe_cast({{ extract_json('parsed_event_data', '$.repayment_rate') }} as numeric) as repayment_rate,
    safe_cast({{ extract_json('parsed_event_data', '$.contractual_repayment_progress') }} as numeric) as contractual_repayment_progress,
    safe_cast({{ extract_json('parsed_event_data', '$.due_repayment_rate') }} as numeric) as due_repayment_rate,
    safe_cast({{ extract_json('parsed_event_data', '$.principal_repayment_rate') }} as numeric) as principal_repayment_rate,
    {{ extract_json('parsed_event_data', '$.repayment_status') }} as repayment_status,
    safe_cast({{ extract_json('parsed_event_data', '$.days_past_due') }} as int64) as days_past_due,
    {{ extract_json('parsed_event_data', '$.delinquency_bucket') }} as delinquency_bucket,
    safe_cast({{ extract_json('parsed_event_data', '$.loan_term_months') }} as int64) as loan_term_months,
    {{ extract_json('parsed_event_data', '$.repayment_frequency') }} as repayment_frequency,
    safe_cast({{ extract_json('parsed_event_data', '$.first_repayment_date') }} as date) as first_repayment_date,
    safe_cast({{ extract_json('parsed_event_data', '$.last_repayment_date') }} as date) as last_repayment_date,
    safe_cast({{ extract_json('parsed_event_data', '$.last_payment_date') }} as date) as last_payment_date,
    {{ extract_json('parsed_event_data', '$.loan_status') }} as loan_status,
    {{ extract_json('parsed_event_data', '$.project_type') }} as project_type,
    {{ extract_json('parsed_event_data', '$.project_location') }} as project_location,
    safe_cast({{ extract_json('parsed_event_data', '$.created_at') }} as timestamp) as created_at,
    safe_cast({{ extract_json('parsed_event_data', '$.updated_at') }} as timestamp) as updated_at,
    _kafka_metadata.topic as kafka_topic,
    _kafka_metadata.`partition` as kafka_partition,
    _kafka_metadata.offset as kafka_offset,
    safe_cast(_kafka_metadata.timestamp as timestamp) as kafka_timestamp,
    _kafka_metadata.category as category,
    'PDMIS' as source_system,
    'loans' as source_group,
    year, month, day,
    event_data, parsed_event_data,
    current_timestamp() as load_timestamp,
    'PDMIS_LOANS' as record_source
from raw_data
where _kafka_metadata.topic = 'pdmis.loans'
  and parsed_event_data is not null
