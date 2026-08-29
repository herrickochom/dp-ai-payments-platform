{{ config(materialized='iceberg_table', tags=['consumption', 'risk', 'identity']) }}

with nin_reuse as (
    select
        nin_hashed,
        count(*) as beneficiary_count,
        count(distinct geography_sk) as geography_count
    from {{ ref('dim_pdm_beneficiary') }}
    where beneficiary_id is not null and nin_hashed is not null
    group by 1
), phone_reuse as (
    select
        phone_hashed,
        count(*) as beneficiary_count,
        count(distinct geography_sk) as geography_count
    from {{ ref('dim_pdm_beneficiary') }}
    where beneficiary_id is not null and phone_hashed is not null
    group by 1
), loans as (
    select
        beneficiary_sk,
        count(*) as loan_count,
        sum(amount_approved) as approved_amount,
        sum(amount_disbursed) as disbursed_amount
    from {{ ref('fct_pdm_loans') }}
    group by 1
), payments as (
    select
        beneficiary_sk,
        count(*) filter (where is_account_substituted) as account_substitution_count,
        sum(payment_amount) filter (where is_account_substituted) as account_substitution_amount
    from {{ ref('fct_pdm_payments') }}
    group by 1
)
select
    beneficiary.beneficiary_sk,
    beneficiary.geography_sk,
    beneficiary.beneficiary_id,
    beneficiary.nin_hashed,
    beneficiary.phone_hashed,
    coalesce(nin_reuse.beneficiary_count, 0) as beneficiaries_per_nin,
    coalesce(nin_reuse.geography_count, 0) as geographies_per_nin,
    coalesce(phone_reuse.beneficiary_count, 0) as beneficiaries_per_phone,
    coalesce(phone_reuse.geography_count, 0) as geographies_per_phone,
    coalesce(loans.loan_count, 0) as loan_count,
    coalesce(loans.approved_amount, 0) as approved_amount,
    coalesce(loans.disbursed_amount, 0) as disbursed_amount,
    coalesce(payments.account_substitution_count, 0) as account_substitution_count,
    coalesce(payments.account_substitution_amount, 0) as account_substitution_amount,
    not coalesce(beneficiary.nin_verified, false) as has_unverified_nin,
    coalesce(nin_reuse.beneficiary_count, 0) > 1 as has_shared_nin,
    coalesce(phone_reuse.beneficiary_count, 0) > 1 as has_shared_phone,
    coalesce(loans.loan_count, 0) > 1 as has_multiple_loans,
    coalesce(payments.account_substitution_count, 0) > 0 as has_account_substitution,
    (case when not coalesce(beneficiary.nin_verified, false) then 1 else 0 end
     + case when coalesce(nin_reuse.beneficiary_count, 0) > 1 then 1 else 0 end
     + case when coalesce(phone_reuse.beneficiary_count, 0) > 1 then 1 else 0 end
     + case when coalesce(loans.loan_count, 0) > 1 then 1 else 0 end
     + case when coalesce(payments.account_substitution_count, 0) > 0 then 1 else 0 end
    ) as identity_alert_count,
    case
        when coalesce(payments.account_substitution_count, 0) > 0
          or coalesce(nin_reuse.beneficiary_count, 0) > 1 then 'HIGH'
        when not coalesce(beneficiary.nin_verified, false)
          or coalesce(phone_reuse.beneficiary_count, 0) > 1
          or coalesce(loans.loan_count, 0) > 1 then 'MEDIUM'
        else 'LOW'
    end as identity_risk_band
from {{ ref('dim_pdm_beneficiary') }} beneficiary
left join nin_reuse using (nin_hashed)
left join phone_reuse using (phone_hashed)
left join loans using (beneficiary_sk)
left join payments using (beneficiary_sk)
where beneficiary.beneficiary_id is not null
