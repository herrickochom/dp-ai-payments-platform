{{ config(materialized='iceberg_table') }}

with staging as (

    select
        event_id,
        message_id,
        end_to_end_id,
        original_transaction_id,
        creation_at,
        transaction_status,
        status_request_id,
        status_reason_code,
        status_additional_info,
        settlement_method,
        clearing_system,
        acceptance_datetime,
        account_servicer_reference,
        clearing_system_reference,
        instructing_agent_bic,
        instructing_agent_name,
        mobile_network,
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
    from {{ ref('stg_pdm_mobile_mtn_pacs002') }}

)

select *
from staging
