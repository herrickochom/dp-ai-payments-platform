{{ config(materialized='iceberg_table', tags=['consumption', 'payments']) }}

select
    payment_date_sk, source_system, currency,
    count(*) as transaction_count,
    sum(payment_amount) as total_payment_amount,
    avg(payment_amount) as average_payment_amount,
    sum(case when is_reconciled then 1 else 0 end) as reconciled_transaction_count,
    sum(case when not is_reconciled then 1 else 0 end) as unreconciled_transaction_count,
    sum(case when is_entity_matched then 1 else 0 end) as entity_matched_transaction_count
from {{ ref('gld_fct_pdm_payments') }}
group by 1, 2, 3
