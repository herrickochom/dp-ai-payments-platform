{{ config(materialized='iceberg_table') }}

select
    {{ gold_surrogate_key(['lifecycle.loan_id']) }} as lifecycle_sk,
    {{ gold_surrogate_key(['lifecycle.beneficiary_id']) }} as beneficiary_sk,
    {{ gold_surrogate_key(['lifecycle.sacco_id']) }} as sacco_sk,
    {{ gold_surrogate_key(['beneficiary.region', 'beneficiary.district', 'beneficiary.parish', 'beneficiary.village']) }} as geography_sk,
    {{ gold_surrogate_key(['cast(lifecycle.approval_date as date)']) }} as approval_date_sk,
    lifecycle.* exclude (beneficiary_id, sacco_id)
from {{ ref('slv_pdm_payment_lifecycle') }} lifecycle
left join {{ ref('slv_pdm_beneficiaries') }} beneficiary using (beneficiary_id)
