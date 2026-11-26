{{ config(materialized='pdm_iceberg_guarded', tags=['consumption', 'payments', 'daily']) }}

-- Grain: one row per reporting date. Only explicit source statuses contribute
-- to success/failure counts; NOT_REPORTED transactions are not failures.
select
    calendar_date as reporting_date,
    count(*) as payment_count,
    countif(is_successful) as successful_payment_count,
    countif(is_failed) as failed_payment_count,
    sum(payment_amount) as payment_amount,
    sum(if(is_successful, payment_amount, null)) as successful_payment_amount,
    sum(if(is_failed, payment_amount, null)) as failed_payment_amount,
    countif(is_reconciled) as reconciled_payment_count,
    countif(is_unreconciled) as unreconciled_payment_count,
    cast(countif(is_reconciled) as float64)
        / nullif(cast(count(*) as float64), 0.0) as reconciliation_rate,
    cast(countif(is_successful) as float64)
        / nullif(cast(count(*) as float64), 0.0) as payment_success_rate,
    count(*) filter (
        where is_failed or is_unreconciled or not is_entity_matched
           or account_substitution_indicator
    ) as operational_exception_count
from {{ ref('cns_pdm_payment_operations') }}
group by 1
