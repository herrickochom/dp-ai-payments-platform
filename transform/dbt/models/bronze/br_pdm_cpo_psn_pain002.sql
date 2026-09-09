{{ config(materialized='iceberg_table') }}

with staging as (

    select
        event_id,
        message_id,
        xml_message_id,
        original_message_id,
        end_to_end_id,
        creation_at,
        xml_creation_at,
        initiating_party,
        original_message_id_flat,
        original_message_type,
        group_status,
        transaction_status,
        xml_group_status,
        xml_transaction_status,
        original_message_name_id,
        original_number_of_transactions,
        original_control_sum,
        original_payment_information_id,
        original_instruction_id,
        original_transaction_id,
        original_uetr,
        reason_code,
        additional_info,
        xml_additional_info,
        settlement_status,
        business_date,
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
    from {{ ref('stg_pdm_cpo_psn_pain002') }}

)

select *
from staging
