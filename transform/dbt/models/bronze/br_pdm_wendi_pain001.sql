{{ config(materialized='iceberg_table') }}

with staging as (

    select
        event_id,
        message_id,
        creation_at,
        number_of_transactions,
        control_sum,
        payment_information_id,
        payment_method,
        instruction_id,
        end_to_end_id,
        transaction_id,
        uetr,
        instructed_amount,
        currency,
        debtor_name,
        debtor_account_id,
        creditor_name,
        creditor_account_id,
        remittance_information,
        complete_message,
        kafka_topic,
        kafka_partition,
        kafka_offset,
        kafka_timestamp,
        year,
        month,
        day,
        load_timestamp,
        record_source
    from {{ ref('stg_pdm_wendi_pain001') }}

)

select *
from staging
