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
 where _kafka_topic = 'pdmis.repayments'
)

select
    event_id,

    {{ extract_json('parsed_event_data', '$.repayment_event_id') }} as repayment_event_id,
    {{ extract_json('parsed_event_data', '$.loan_id') }} as loan_id,
    {{ extract_json('parsed_event_data', '$.beneficiary_id') }} as beneficiary_id,
    {{ extract_json('parsed_event_data', '$.sacco_id') }} as sacco_id,

    safe_cast({{ extract_json('parsed_event_data', '$.event_sequence') }} as int64) as event_sequence,
    {{ extract_json('parsed_event_data', '$.event_type') }} as event_type,

    safe_cast({{ extract_json('parsed_event_data', '$.event_date') }} as date) as event_date,
    safe_cast({{ extract_json('parsed_event_data', '$.payment_date') }} as date) as payment_date,
    safe_cast({{ extract_json('parsed_event_data', '$.repayment_due_date') }} as date) as repayment_due_date,
    safe_cast({{ extract_json('parsed_event_data', '$.as_of_date') }} as date) as as_of_date,

    safe_cast({{ extract_json('parsed_event_data', '$.instalment_number') }} as int64) as instalment_number,

    safe_cast({{ extract_json('parsed_event_data', '$.amount') }} as numeric) as amount,
    {{ extract_json('parsed_event_data', '$.currency') }} as currency,
    {{ extract_json('parsed_event_data', '$.channel') }} as channel,
    {{ extract_json('parsed_event_data', '$.payment_status') }} as payment_status,

    safe_cast({{ extract_json('parsed_event_data', '$.scheduled_amount') }} as numeric) as scheduled_amount,
    safe_cast({{ extract_json('parsed_event_data', '$.scheduled_principal_amount') }} as numeric) as scheduled_principal_amount,
    safe_cast({{ extract_json('parsed_event_data', '$.scheduled_interest_amount') }} as numeric) as scheduled_interest_amount,

    safe_cast({{ extract_json('parsed_event_data', '$.cumulative_amount_paid') }} as numeric) as cumulative_amount_paid,
    safe_cast({{ extract_json('parsed_event_data', '$.principal_paid') }} as numeric) as principal_paid,
    safe_cast({{ extract_json('parsed_event_data', '$.interest_paid') }} as numeric) as interest_paid,

    safe_cast({{ extract_json('parsed_event_data', '$.principal_outstanding_balance') }} as numeric) as principal_outstanding_balance,
    safe_cast({{ extract_json('parsed_event_data', '$.interest_outstanding_balance') }} as numeric) as interest_outstanding_balance,
    safe_cast({{ extract_json('parsed_event_data', '$.outstanding_balance') }} as numeric) as outstanding_balance,

    safe_cast({{ extract_json('parsed_event_data', '$.repayment_rate') }} as numeric) as repayment_rate,
    safe_cast({{ extract_json('parsed_event_data', '$.contractual_repayment_progress') }} as numeric) as contractual_repayment_progress,
    safe_cast({{ extract_json('parsed_event_data', '$.due_repayment_rate') }} as numeric) as due_repayment_rate,
    safe_cast({{ extract_json('parsed_event_data', '$.principal_repayment_rate') }} as numeric) as principal_repayment_rate,

    safe_cast({{ extract_json('parsed_event_data', '$.days_late') }} as int64) as days_late,
    safe_cast({{ extract_json('parsed_event_data', '$.days_past_due') }} as int64) as days_past_due,
    {{ extract_json('parsed_event_data', '$.delinquency_bucket') }} as delinquency_bucket,

    _kafka_metadata.topic as kafka_topic,
    _kafka_metadata.`partition` as kafka_partition,
    _kafka_metadata.offset as kafka_offset,
    safe_cast(_kafka_metadata.timestamp as timestamp) as kafka_timestamp,
    _kafka_metadata.category as category,

    'PDMIS' as source_system,
    'repayments' as source_group,

    year, month, day,
    event_data, parsed_event_data,

    current_timestamp() as load_timestamp,
    'PDMIS_REPAYMENTS' as record_source

from raw_data
where _kafka_metadata.topic = 'pdmis.repayments'
  and parsed_event_data is not null
