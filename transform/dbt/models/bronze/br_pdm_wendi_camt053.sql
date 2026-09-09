{{ config(materialized='iceberg_table') }}

with staging as (

    select
        event_id,
        message_id,
        end_to_end_id,
        transaction_id,
        statement_id,
        message_created_at,
        statement_created_at,
        electronic_sequence_number,
        period_from_at,
        period_to_at,
        account_id,
        account_issuer,
        balance_type,
        balance_amount,
        currency,
        credit_debit_indicator,
        balance_date,
        entry_amount,
        entry_currency,
        entry_credit_debit_indicator,
        entry_status,
        entry_reference,
        account_servicer_reference,
        booking_date,
        value_date,
        bank_transaction_code,
        instruction_id,
        uetr,
        transaction_amount,
        transaction_currency,
        transaction_credit_debit_indicator,
        debtor_name,
        creditor_name,
        debtor_agent_bic,
        debtor_agent_name,
        creditor_agent_bic,
        creditor_agent_name,
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
    from {{ ref('stg_pdm_wendi_camt053') }}

)

select *
from staging
