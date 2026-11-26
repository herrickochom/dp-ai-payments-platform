{{ config(materialized='pdm_iceberg_guarded', tags=['consumption', 'operations', 'reconciliation']) }}

-- Grain: one row per reporting date. Stage counts are transparent counts of
-- observed source-system transactions, not reconstructed accounting entries.
select
    calendar_date as reporting_date,
    count(*) as payment_count,
    sum(payment_amount) as payment_amount,
    countif(source_system = 'ICMN_VPM') as initiated_count,
    countif(source_system in ('MTN_PACS008', 'AIRTEL_PACS008') and is_successful) as settled_count,
    countif(source_system = 'WENDI_WALLET' and is_successful) as credited_count,
    countif(source_system = 'AGENT' and is_successful) as disbursed_count,
    countif(is_reconciled) as reconciled_count,
    countif(is_unreconciled) as unreconciled_count,
    sum(if(is_reconciled, payment_amount, null)) as reconciled_amount,
    sum(if(is_unreconciled, payment_amount, null)) as unreconciled_amount,
    cast(countif(is_reconciled) as float64)
        / nullif(cast(count(*) as float64), 0.0) as reconciliation_rate,
    countif(is_unreconciled or not is_entity_matched or is_failed) as exception_count
from {{ ref('cns_pdm_payment_operations') }}
group by 1
