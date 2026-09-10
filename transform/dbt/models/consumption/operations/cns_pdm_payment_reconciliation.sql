{{ config(materialized='iceberg_table', tags=['consumption', 'operations', 'reconciliation']) }}

-- Grain: one row per reporting date. Stage counts are transparent counts of
-- observed source-system transactions, not reconstructed accounting entries.
select
    calendar_date as reporting_date,
    count(*) as payment_count,
    sum(payment_amount) as payment_amount,
    count(*) filter (where source_system = 'ICMN_VPM') as initiated_count,
    count(*) filter (where source_system in ('MTN_PACS008', 'AIRTEL_PACS008') and is_successful) as settled_count,
    count(*) filter (where source_system = 'WENDI_WALLET' and is_successful) as credited_count,
    count(*) filter (where source_system = 'AGENT' and is_successful) as disbursed_count,
    count(*) filter (where is_reconciled) as reconciled_count,
    count(*) filter (where is_unreconciled) as unreconciled_count,
    sum(payment_amount) filter (where is_reconciled) as reconciled_amount,
    sum(payment_amount) filter (where is_unreconciled) as unreconciled_amount,
    cast(count(*) filter (where is_reconciled) as double)
        / nullif(cast(count(*) as double), 0.0) as reconciliation_rate,
    count(*) filter (where is_unreconciled or not is_entity_matched or is_failed) as exception_count
from {{ ref('cns_pdm_payment_operations') }}
group by 1
