{{ config(materialized='iceberg_table', tags=['consumption', 'payments', 'daily']) }}

-- Grain: one row per reporting date. Only explicit source statuses contribute
-- to success/failure counts; NOT_REPORTED transactions are not failures.
select
    calendar_date as reporting_date,
    count(*) as payment_count,
    count(*) filter (where is_successful) as successful_payment_count,
    count(*) filter (where is_failed) as failed_payment_count,
    sum(payment_amount) as payment_amount,
    sum(payment_amount) filter (where is_successful) as successful_payment_amount,
    sum(payment_amount) filter (where is_failed) as failed_payment_amount,
    count(*) filter (where is_reconciled) as reconciled_payment_count,
    count(*) filter (where is_unreconciled) as unreconciled_payment_count,
    cast(count(*) filter (where is_reconciled) as double)
        / nullif(cast(count(*) as double), 0.0) as reconciliation_rate,
    cast(count(*) filter (where is_successful) as double)
        / nullif(cast(count(*) as double), 0.0) as payment_success_rate,
    count(*) filter (
        where is_failed or is_unreconciled or not is_entity_matched
           or account_substitution_indicator
    ) as operational_exception_count
from {{ ref('cns_pdm_payment_operations') }}
group by 1
