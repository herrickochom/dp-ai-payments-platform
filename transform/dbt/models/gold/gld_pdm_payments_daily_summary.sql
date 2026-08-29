{{ config(materialized='iceberg_table') }}

select
    cast(creation_at as date) as payment_date,
    source_system,
    currency,
    count(*) as transaction_count,
    sum(instructed_amount) as total_instructed_amount,
    avg(instructed_amount) as average_instructed_amount
from {{ ref('slv_pdm_payments_transactions') }}
group by 1, 2, 3
