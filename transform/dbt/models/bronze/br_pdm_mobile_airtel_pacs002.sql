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
        transaction_x_correlationid,
        transaction_x_environment,
        transaction_x_flags,
        transaction_x_messagetype,
        transaction_x_messageversion,
        transaction_x_parentspanid,
        transaction_x_processingnode,
        transaction_x_processingpriority,
        transaction_x_requestid,
        transaction_x_retrycount,
        transaction_x_sampled,
        transaction_x_spanid,
        transaction_x_tenantid,
        transaction_x_timeout,
        transaction_x_timestamp,
        transaction_x_traceid,
        transaction_x_version,
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
    from {{ ref('stg_pdm_mobile_airtel_pacs002') }}

)

select *
from staging
