{{ config(materialized='iceberg_table') }}

with staging as (

    select
        event_id,
        message_id,
        creation_at,
        original_message_id,
        group_status,
        original_payment_information_id,
        end_to_end_id,
        original_transaction_id,
        transaction_status,
        status_reason_code,
        status_additional_info,
        complete_status_report,
        kafka_topic,
        kafka_partition,
        kafka_offset,
        kafka_timestamp,
        year,
        month,
        day,
        load_timestamp,
        record_source
    from {{ ref('stg_pdm_wendi_pain002') }}

)

select *
from staging
