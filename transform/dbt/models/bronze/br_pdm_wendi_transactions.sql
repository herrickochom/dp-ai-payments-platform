{{ config(materialized='iceberg_table') }}

with staging as (

    select
        event_id,
        wallet_event_id,
        loan_id,
        beneficiary_id,
        beneficiary_name,
        sacco_id,
        event_timestamp,
        event_type,
        source_account,
        source_account_type,
        destination_account,
        destination_account_type,
        amount,
        currency,
        transaction_status,
        wendi_transaction_id,
        agent_id,
        device_id,
        ip_address,
        user_agent,
        metadata_source_system,
        metadata_transaction_type,
        metadata_is_assisted,
        metadata_processing_time_ms,
        created_at,
        updated_at,
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
    from {{ ref('stg_pdm_wendi_transactions') }}

)

select *
from staging
