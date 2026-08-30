{{ config(materialized='iceberg_table', tags=['consumption', 'operations']) }}

select
    payment.payment_date_sk,
    date.calendar_date,
    payment.source_system,
    payment.currency,
    count(*) as transaction_count,
    sum(payment.payment_amount) as total_payment_amount,
    avg(payment.payment_amount) as average_payment_amount,
    count(*) filter (where payment.is_reconciled) as reconciled_transaction_count,
    count(*) filter (where not payment.is_reconciled) as unreconciled_transaction_count,
    count(*) filter (where payment.is_entity_matched) as matched_entity_count,
    count(*) filter (where not payment.is_entity_matched) as unmatched_entity_count,
    count(*) filter (where payment.transaction_status = 'RJCT') as rejected_transaction_count,
    count(*) filter (where payment.is_account_substituted) as account_substitution_count,
    count(*) filter (where payment.is_reconciled) / nullif(count(*), 0) as reconciliation_rate,
    count(*) filter (where payment.is_entity_matched) / nullif(count(*), 0) as entity_match_rate
from {{ ref('gld_fct_pdm_payments') }} payment
left join {{ ref('gld_dim_pdm_date') }} date on payment.payment_date_sk = date.date_sk
group by 1, 2, 3, 4
