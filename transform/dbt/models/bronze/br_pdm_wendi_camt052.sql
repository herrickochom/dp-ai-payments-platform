{{ config(materialized='iceberg_table') }}

with staging as (

    select
        event_id,
        message_id,
        report_created_at,
        account_id,
        account_issuer,
        balance_type,
        balance_amount,
        currency,
        credit_debit_indicator,
        balance_date,
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
    from {{ ref('stg_pdm_wendi_camt052') }}

)

select *
from staging
