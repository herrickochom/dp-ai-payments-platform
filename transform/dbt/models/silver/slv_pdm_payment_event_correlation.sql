{{ config(materialized='iceberg_table') }}

with technical as (
    select * from {{ ref('slv_pdm_payment_technical_events') }}
), business as (
    select * from {{ ref('slv_pdm_payments_transactions') }}
), candidates as (
    select t.event_id as technical_event_id, b.source_system as business_source_system,
           b.message_id as business_message_id, b.transaction_id as business_transaction_id,
           'UETR' as correlation_method, 1 as match_priority
    from technical t join business b on t.uetr is not null and t.uetr = b.uetr
    union all
    select t.event_id, b.source_system, b.message_id, b.transaction_id,
           'TRANSACTION_ID', 2
    from technical t join business b
      on t.transaction_id is not null and t.transaction_id = b.transaction_id
    union all
    select t.event_id, b.source_system, b.message_id, b.transaction_id,
           'END_TO_END_ID', 3
    from technical t join business b
      on t.end_to_end_id is not null and t.end_to_end_id = b.end_to_end_id
    union all
    select t.event_id, b.source_system, b.message_id, b.transaction_id,
           'INSTRUCTION_ID', 4
    from technical t join business b
      on t.instruction_id is not null and t.instruction_id = b.instruction_id
), ranked as (
    select *, row_number() over (
        partition by technical_event_id
        order by match_priority,
                 case business_source_system
                   when 'ICMN_VPM' then 1 when 'WENDI_PAIN001' then 2
                   when 'MTN_PACS008' then 3 when 'AIRTEL_PACS008' then 4 else 5
                 end,
                 business_message_id
    ) as match_rank
    from candidates
)
select
    t.event_id as technical_event_id,
    t.technical_source,
    t.event_type,
    t.correlation_id,
    t.instruction_id,
    t.end_to_end_id,
    t.transaction_id as technical_transaction_id,
    t.uetr,
    t.business_reference,
    t.technical_status,
    t.event_timestamp,
    r.business_source_system,
    r.business_message_id,
    r.business_transaction_id,
    r.correlation_method,
    case when r.technical_event_id is null then 'UNMATCHED' else 'MATCHED' end as match_status
from technical t
left join ranked r
  on t.event_id = r.technical_event_id and r.match_rank = 1

