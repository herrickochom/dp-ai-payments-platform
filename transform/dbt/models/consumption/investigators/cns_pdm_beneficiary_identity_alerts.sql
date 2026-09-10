{{ config(materialized='iceberg_table', tags=['consumption', 'risk', 'identity']) }}

with nin_reuse as (
    select
        nin_hashed,
        count(*) as beneficiary_count,
        count(distinct geography_sk) as geography_count
    from {{ ref('gld_dim_pdm_beneficiary') }}
    where beneficiary_id is not null and nin_hashed is not null
    group by 1
), phone_reuse as (
    select
        phone_hashed,
        count(*) as beneficiary_count,
        count(distinct geography_sk) as geography_count
    from {{ ref('gld_dim_pdm_beneficiary') }}
    where beneficiary_id is not null and phone_hashed is not null
    group by 1
), loans as (
    select
        beneficiary_sk,
        count(*) as loan_count,
        sum(amount_approved) as approved_amount,
        sum(amount_disbursed) as disbursed_amount
    from {{ ref('gld_fct_pdm_loans') }}
    group by 1
), account_reuse as (
    select
        creditor_account_hashed,
        count(distinct beneficiary_sk) as beneficiary_count
    from {{ ref('gld_fct_pdm_payments') }}
    where creditor_account_hashed is not null
    group by 1
), payments as (
    select
        payment.beneficiary_sk,
        count(*) filter (where payment.is_account_substituted) as account_substitution_count,
        sum(payment.payment_amount) filter (where payment.is_account_substituted) as account_substitution_amount,
        count(*) filter (where coalesce(account.beneficiary_count, 0) > 1) as shared_account_payment_count
    from {{ ref('gld_fct_pdm_payments') }} payment
    left join account_reuse account using (creditor_account_hashed)
    group by 1
)
select
    beneficiary.beneficiary_sk,
    beneficiary.geography_sk,
    beneficiary.beneficiary_id,
    geography.region,
    geography.district,
    geography.county,
    geography.sub_county,
    geography.parish,
    geography.village,
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
    coalesce(payments.shared_account_payment_count, 0) > 0 as has_shared_account,
    (case when not coalesce(beneficiary.nin_verified, false) then 1 else 0 end
     + case when coalesce(nin_reuse.beneficiary_count, 0) > 1 then 1 else 0 end
     + case when coalesce(phone_reuse.beneficiary_count, 0) > 1 then 1 else 0 end
     + case when coalesce(loans.loan_count, 0) > 1 then 1 else 0 end
     + case when coalesce(payments.account_substitution_count, 0) > 0 then 1 else 0 end
     + case when coalesce(payments.shared_account_payment_count, 0) > 0 then 1 else 0 end
    ) as identity_alert_count,
    case
        when coalesce(payments.account_substitution_count, 0) > 0
          or coalesce(nin_reuse.beneficiary_count, 0) > 1 then 'HIGH'
        when not coalesce(beneficiary.nin_verified, false)
          or coalesce(phone_reuse.beneficiary_count, 0) > 1
          or coalesce(loans.loan_count, 0) > 1
          or coalesce(payments.shared_account_payment_count, 0) > 0 then 'MEDIUM'
        else 'LOW'
    end as identity_risk_band,
    concat_ws('; ',
        case when not coalesce(beneficiary.nin_verified, false) then 'NIN_UNVERIFIED' end,
        case when coalesce(nin_reuse.beneficiary_count, 0) > 1 then 'SHARED_IDENTITY' end,
        case when coalesce(phone_reuse.beneficiary_count, 0) > 1 then 'SHARED_PHONE_TOKEN' end,
        case when coalesce(loans.loan_count, 0) > 1 then 'MULTIPLE_LOANS' end,
        case when coalesce(payments.account_substitution_count, 0) > 0 then 'ACCOUNT_SUBSTITUTION' end,
        case when coalesce(payments.shared_account_payment_count, 0) > 0 then 'SHARED_ACCOUNT_TOKEN' end
    ) as identity_alert_reason,
    case
        when coalesce(payments.account_substitution_count, 0) > 0
          or coalesce(nin_reuse.beneficiary_count, 0) > 1 then 'HIGH'
        when not coalesce(beneficiary.nin_verified, false)
          or coalesce(phone_reuse.beneficiary_count, 0) > 1
          or coalesce(loans.loan_count, 0) > 1
          or coalesce(payments.shared_account_payment_count, 0) > 0 then 'MEDIUM'
        else 'LOW'
    end as identity_alert_severity,
    (not coalesce(beneficiary.nin_verified, false)
      or coalesce(nin_reuse.beneficiary_count, 0) > 1
      or coalesce(phone_reuse.beneficiary_count, 0) > 1
      or coalesce(loans.loan_count, 0) > 1
      or coalesce(payments.account_substitution_count, 0) > 0
      or coalesce(payments.shared_account_payment_count, 0) > 0) as requires_investigation,
    'INDICATOR_NOT_FRAUD_DETERMINATION' as interpretation
from {{ ref('gld_dim_pdm_beneficiary') }} beneficiary
left join {{ ref('gld_dim_pdm_geography') }} geography using (geography_sk)
left join nin_reuse using (nin_hashed)
left join phone_reuse using (phone_hashed)
left join loans using (beneficiary_sk)
left join payments using (beneficiary_sk)
where beneficiary.beneficiary_id is not null
