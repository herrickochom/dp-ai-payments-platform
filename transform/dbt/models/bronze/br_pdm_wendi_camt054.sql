{{ config(materialized='iceberg_table') }}

with staging as (

    select
        event_id,
        message_id,
        message_created_at,
        end_to_end_id,
        notification_id,
        notification_created_at,
        account_id,
        account_issuer,
        entry_amount,
        currency,
        credit_debit_indicator,
        entry_status,
        booking_date,
        transaction_id,
        transaction_amount,
        transaction_currency,
        transaction_credit_debit_indicator,
        remittance_information,
        kafka_topic,
        kafka_partition,
        kafka_offset,
        kafka_timestamp,
        category,
        source_system,
        source_group,
        year,
        month,
        day,
        load_timestamp,
        record_source
    from {{ ref('stg_pdm_wendi_camt054') }}

)

select *
from staging
