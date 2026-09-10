{{ config(materialized='iceberg_table', tags=['consumption', 'operations', 'payments']) }}

-- Grain: one row per source-system payment transaction. The former aggregate
-- grain could not support an operational payment identifier or case follow-up.
-- Status reports are reduced to one row per original transaction before join.
with reported_status as (
    select
        original_transaction_id as transaction_id,
        max(transaction_status) filter (where transaction_status is not null) as transaction_status,
        max(group_status) filter (where group_status is not null) as group_status
    from {{ ref('slv_pdm_payments_status_report') }}
    where original_transaction_id is not null
    group by 1
), payments as (
    select
        payment.*,
        coalesce(payment.transaction_status, status.transaction_status, status.group_status)
            as resolved_transaction_status
    from {{ ref('gld_fct_pdm_payments') }} payment
    left join reported_status status using (transaction_id)
)
select
    payment.payment_sk,
    payment.payment_date_sk,
    payment.transaction_id as payment_id,
    payment.transaction_id,
    payment.beneficiary_sk,
    beneficiary.beneficiary_id,
    payment.loan_id,
    payment.sacco_sk,
    sacco.sacco_id,
    sacco.sacco_name,
    payment.geography_sk,
    geography.region,
    geography.district,
    date.calendar_date,
    payment.occurred_at,
    payment.source_system,
    payment.currency,
    payment.payment_amount,
    payment.transaction_status as source_transaction_status,
    payment.resolved_transaction_status as transaction_status,
    case
        when payment.resolved_transaction_status in ('ACSC', 'COMPLETED', 'SUCCESSFUL') then 'SUCCESSFUL'
        when payment.resolved_transaction_status in ('RJCT', 'FAILED') then 'FAILED'
        when payment.resolved_transaction_status = 'PDNG' then 'PENDING'
        else 'NOT_REPORTED'
    end as payment_status,
    payment.entity_match_status,
    case when payment.is_reconciled then 'RECONCILED' else 'UNRECONCILED' end
        as reconciliation_status,
    payment.is_account_substituted as account_substitution_indicator,
    payment.resolved_transaction_status in ('ACSC', 'COMPLETED', 'SUCCESSFUL') as is_successful,
    payment.resolved_transaction_status in ('RJCT', 'FAILED') as is_failed,
    payment.is_reconciled,
    not payment.is_reconciled as is_unreconciled,
    payment.is_entity_matched,
    1 as payment_count
from payments payment
left join {{ ref('gld_dim_pdm_date') }} date
  on payment.payment_date_sk = date.date_sk
left join {{ ref('gld_dim_pdm_beneficiary') }} beneficiary
  on payment.beneficiary_sk = beneficiary.beneficiary_sk
left join {{ ref('gld_dim_pdm_sacco') }} sacco
  on payment.sacco_sk = sacco.sacco_sk
left join {{ ref('gld_dim_pdm_geography') }} geography
  on payment.geography_sk = geography.geography_sk
