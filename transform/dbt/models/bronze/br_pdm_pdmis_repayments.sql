{{ config(enabled=false) }}

{#
  Raw -> Bronze transformation definition.

  Execution ownership:
      Raw Avro -> DuckDB -> bulk Parquet
      -> transform-Trino -> Iceberg/Nessie Bronze

  This model is intentionally disabled in dbt because read_avro() and
  extract_json() are executed by DuckDB, not Trino.
#}


with raw_data as (
    select event_id, event_data, parsed_event_data, _kafka_metadata, year, month, day
    from read_avro(
        's3://{{ var("s3_bucket") }}/{{ var("s3_path") }}/v2/**/topic=pdmis.repayments/**/*.avro',
        hive_partitioning = true
    )
)

select
    event_id,

    {{ extract_json('parsed_event_data', '$.repayment_event_id') }} as repayment_event_id,
    {{ extract_json('parsed_event_data', '$.loan_id') }} as loan_id,
    {{ extract_json('parsed_event_data', '$.beneficiary_id') }} as beneficiary_id,
    {{ extract_json('parsed_event_data', '$.sacco_id') }} as sacco_id,

    try_cast({{ extract_json('parsed_event_data', '$.event_sequence') }} as integer) as event_sequence,
    {{ extract_json('parsed_event_data', '$.event_type') }} as event_type,

    try_cast({{ extract_json('parsed_event_data', '$.event_date') }} as date) as event_date,
    try_cast({{ extract_json('parsed_event_data', '$.payment_date') }} as date) as payment_date,
    try_cast({{ extract_json('parsed_event_data', '$.repayment_due_date') }} as date) as repayment_due_date,
    try_cast({{ extract_json('parsed_event_data', '$.as_of_date') }} as date) as as_of_date,

    try_cast({{ extract_json('parsed_event_data', '$.instalment_number') }} as integer) as instalment_number,

    try_cast({{ extract_json('parsed_event_data', '$.amount') }} as decimal(18, 2)) as amount,
    {{ extract_json('parsed_event_data', '$.currency') }} as currency,
    {{ extract_json('parsed_event_data', '$.channel') }} as channel,
    {{ extract_json('parsed_event_data', '$.payment_status') }} as payment_status,

    try_cast({{ extract_json('parsed_event_data', '$.scheduled_amount') }} as decimal(18, 2)) as scheduled_amount,
    try_cast({{ extract_json('parsed_event_data', '$.scheduled_principal_amount') }} as decimal(18, 2)) as scheduled_principal_amount,
    try_cast({{ extract_json('parsed_event_data', '$.scheduled_interest_amount') }} as decimal(18, 2)) as scheduled_interest_amount,

    try_cast({{ extract_json('parsed_event_data', '$.cumulative_amount_paid') }} as decimal(18, 2)) as cumulative_amount_paid,
    try_cast({{ extract_json('parsed_event_data', '$.principal_paid') }} as decimal(18, 2)) as principal_paid,
    try_cast({{ extract_json('parsed_event_data', '$.interest_paid') }} as decimal(18, 2)) as interest_paid,

    try_cast({{ extract_json('parsed_event_data', '$.principal_outstanding_balance') }} as decimal(18, 2)) as principal_outstanding_balance,
    try_cast({{ extract_json('parsed_event_data', '$.interest_outstanding_balance') }} as decimal(18, 2)) as interest_outstanding_balance,
    try_cast({{ extract_json('parsed_event_data', '$.outstanding_balance') }} as decimal(18, 2)) as outstanding_balance,

    try_cast({{ extract_json('parsed_event_data', '$.repayment_rate') }} as decimal(18, 6)) as repayment_rate,
    try_cast({{ extract_json('parsed_event_data', '$.contractual_repayment_progress') }} as decimal(18, 6)) as contractual_repayment_progress,
    try_cast({{ extract_json('parsed_event_data', '$.due_repayment_rate') }} as decimal(18, 6)) as due_repayment_rate,
    try_cast({{ extract_json('parsed_event_data', '$.principal_repayment_rate') }} as decimal(18, 6)) as principal_repayment_rate,

    try_cast({{ extract_json('parsed_event_data', '$.days_late') }} as integer) as days_late,
    try_cast({{ extract_json('parsed_event_data', '$.days_past_due') }} as integer) as days_past_due,
    {{ extract_json('parsed_event_data', '$.delinquency_bucket') }} as delinquency_bucket,

    _kafka_metadata.topic as kafka_topic,
    _kafka_metadata.partition as kafka_partition,
    _kafka_metadata.offset as kafka_offset,
    try_cast(_kafka_metadata.timestamp as timestamp) as kafka_timestamp,
    _kafka_metadata.category as category,

    'PDMIS' as source_system,
    'repayments' as source_group,

    year, month, day,
    event_data, parsed_event_data,

    current_timestamp as load_timestamp,
    'PDMIS_REPAYMENTS' as record_source

from raw_data
where _kafka_metadata.topic = 'pdmis.repayments'
  and parsed_event_data is not null
