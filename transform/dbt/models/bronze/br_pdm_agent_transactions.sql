{{ config(materialized='iceberg_table') }}

with staging as (

    select
        event_id,
        transaction_id,
        agent_id,
        loan_id,
        beneficiary_id,
        beneficiary_name,
        transaction_timestamp,
        transaction_type,
        amount,
        fee_amount,
        is_assisted_withdrawal,
        beneficiary_verified,
        verification_method,
        status,
        reference,
        location_latitude,
        location_longitude,
        location_address,
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
    from {{ ref('stg_pdm_agent_transactions') }}

)

select *
from staging
