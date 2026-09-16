{{ config(materialized='iceberg_table', tags=['gold', 'privacy-boundary']) }}

-- GATE 2 PRIVACY BOUNDARY: accumulating loan control fact keyed to the
-- canonical beneficiary_token via the pseudonymous loan linkage, carrying only
-- approved coarse geography from Silver.

select
    {{ gold_surrogate_key(['lifecycle.loan_id']) }} as lifecycle_sk,
    {{ gold_surrogate_key(['loan_link.beneficiary_token']) }} as beneficiary_sk,
    {{ gold_surrogate_key(['lifecycle.sacco_id']) }} as sacco_sk,
    {{ gold_surrogate_key(['beneficiary.region', 'beneficiary.district', 'beneficiary.county', 'beneficiary.sub_county']) }} as geography_sk,
    {{ gold_surrogate_key(['cast(lifecycle.approval_date as date)']) }} as approval_date_sk,
    lifecycle.* exclude (beneficiary_id, sacco_id),
    loan_link.beneficiary_token,
    beneficiary.region,
    beneficiary.district,
    beneficiary.county,
    beneficiary.sub_county
from {{ ref('slv_pdm_payment_lifecycle') }} lifecycle
left join {{ ref('slv_pdm_loan_beneficiary_links') }} loan_link
    on lifecycle.loan_id = loan_link.loan_id
left join {{ ref('slv_pdm_beneficiaries') }} beneficiary
    on loan_link.beneficiary_token = beneficiary.beneficiary_token
