{{ config(materialized='iceberg_table') }}

with staging as (

    select
        event_id,
        message_id,
        end_to_end_id,
        creation_at,
        xml_creation_at,
        instructed_amount,
        currency,
        debtor_name,
        creditor_name,
        xml_debtor_name,
        xml_creditor_name,
        xml_instructed_amount,
        xml_currency,
        number_of_transactions,
        group_control_sum,
        initiating_party,
        payment_information_id,
        payment_method,
        payment_number_of_transactions,
        payment_control_sum,
        batch_booking,
        requested_execution_at,
        debtor_account_id,
        debtor_agent_id,
        creditor_account_id,
        creditor_agent_id,
        debtor_account_issuer,
        creditor_account_issuer,
        creditor_account_scheme,
        instruction_id,
        transaction_id,
        uetr,
        remittance_information,
        purpose_code,
        invoice_reference_1,
        invoice_reference_2,
        invoice_reference_3,
        invoice_reference_4,
        invoice_reference_5,
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
    from {{ ref('stg_pdm_icmn_vpm_pain001') }}

)

select *
from staging
