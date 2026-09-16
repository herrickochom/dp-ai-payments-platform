{{ config(
    materialized='iceberg_table',
    tags=['silver_vault', 'identity', 'restricted', 'alerts'],
    meta={'classification': 'restricted'}
) }}

-- RESTRICTED IDENTITY ALERTS (relocated from ordinary consumption per Gate 2).
-- Same indicator semantics as the former consumption product (shared NIN,
-- shared phone, unverified NIN, multiple loans, payment-account substitution)
-- but computed from the RESTRICTED identity vault so the clear-identity
-- correlators (identity hashes, contact data, fine-grained geography) never
-- leave the vault boundary. Ordinary layers consume only the pseudonymous
-- per-token signal projection (vlt_pdm_beneficiary_identity_signals).
-- Interpretation is unchanged: INDICATOR_NOT_FRAUD_DETERMINATION.

with identity as (

    select * from {{ ref('vlt_pdm_beneficiary_identity') }}

),

nin_reuse as (
    select
        nin_hashed,
        count(*) as beneficiary_count,
        count(distinct concat_ws('||', region, district, county, sub_county, parish, village))
            as geography_count
    from identity
    where beneficiary_id is not null and nin_hashed is not null
    group by 1
),

phone_reuse as (
    select
        phone_hashed,
        count(*) as beneficiary_count,
        count(distinct concat_ws('||', region, district, county, sub_county, parish, village))
            as geography_count
    from identity
    where beneficiary_id is not null and phone_hashed is not null
    group by 1
),

loans as (
    select
        beneficiary_id,
        count(*) as loan_count,
        sum(amount_approved) as approved_amount,
        sum(amount_disbursed) as disbursed_amount
    from {{ ref('slv_pdm_loans') }}
    where beneficiary_id is not null
    group by 1
),

-- Restricted creditor-account correlator: plain SHA-256 of the normalised
-- account digits is retained ONLY inside this RESTRICTED vault product as a
-- reuse-detection grouping key. It is not an analytical token and is never
-- projected to ordinary layers (which see only substitution booleans).
account_link as (
    select
        t.source_system,
        t.transaction_id,
        t.end_to_end_id,
        case
            when nullif(regexp_replace(coalesce(pr.account_id, ''), '[^0-9]', ''), '') is null
                then null
            else sha256(regexp_replace(pr.account_id, '[^0-9]', ''))
        end as creditor_account_correlator
    from {{ ref('slv_pdm_payments_transactions') }} t
    join {{ ref('slv_pdm_payment_party_roles') }} pr
        on t.message_id = pr.message_id and pr.party_role = 'CREDITOR'
    where t.end_to_end_id is not null
),

account_reuse as (
    select
        account_link.creditor_account_correlator,
        count(distinct identity.beneficiary_id) as beneficiary_count
    from account_link
    join {{ ref('slv_pdm_loan_beneficiary_links') }} loan_link
        on account_link.end_to_end_id = loan_link.loan_id
    join identity
        on loan_link.beneficiary_token = identity.beneficiary_token
    where account_link.creditor_account_correlator is not null
    group by 1
),

account_substitution as (
    select
        loan_link.beneficiary_token,
        count(*) filter (where controls.is_account_substituted) as account_substitution_count,
        sum(t.amount) filter (where controls.is_account_substituted)
            as account_substitution_amount,
        count(*) filter (where coalesce(reuse.beneficiary_count, 0) > 1)
            as shared_account_payment_count
    from {{ ref('slv_pdm_payments_transactions') }} t
    join {{ ref('slv_pdm_loan_beneficiary_links') }} loan_link
        on t.end_to_end_id = loan_link.loan_id
    left join {{ ref('slv_pdm_payment_account_controls') }} controls
        on t.source_system = controls.source_system
        and t.transaction_id = controls.transaction_id
    left join account_link
        on t.source_system = account_link.source_system
        and t.transaction_id = account_link.transaction_id
    left join account_reuse reuse
        on account_link.creditor_account_correlator = reuse.creditor_account_correlator
    group by 1
)

select
    {{ gold_surrogate_key(['identity.beneficiary_token']) }} as beneficiary_sk,
    identity.beneficiary_id,
    identity.beneficiary_token,
    identity.token_version,
    identity.legacy_source_beneficiary_token,
    identity.beneficiary_name,
    identity.nin,
    identity.nin_hashed,
    identity.phone_hashed,
    identity.date_of_birth,
    identity.age_band,
    identity.gender,
    identity.phone,
    identity.alternative_phone,
    identity.email,
    identity.household_id,
    identity.village,
    identity.parish,
    identity.sub_county,
    identity.county,
    identity.district,
    identity.region,
    coalesce(nin_reuse.beneficiary_count, 0) as beneficiaries_per_nin,
    coalesce(nin_reuse.geography_count, 0) as geographies_per_nin,
    coalesce(phone_reuse.beneficiary_count, 0) as beneficiaries_per_phone,
    coalesce(phone_reuse.geography_count, 0) as geographies_per_phone,
    coalesce(loans.loan_count, 0) as loan_count,
    coalesce(loans.approved_amount, 0) as approved_amount,
    coalesce(loans.disbursed_amount, 0) as disbursed_amount,
    coalesce(substitution.account_substitution_count, 0) as account_substitution_count,
    coalesce(substitution.account_substitution_amount, 0) as account_substitution_amount,
    not coalesce(identity.nin_verified, false) as has_unverified_nin,
    coalesce(nin_reuse.beneficiary_count, 0) > 1 as has_shared_nin,
    coalesce(phone_reuse.beneficiary_count, 0) > 1 as has_shared_phone,
    coalesce(loans.loan_count, 0) > 1 as has_multiple_loans,
    coalesce(substitution.account_substitution_count, 0) > 0 as has_account_substitution,
    coalesce(substitution.shared_account_payment_count, 0) > 0 as has_shared_account,
    (case when not coalesce(identity.nin_verified, false) then 1 else 0 end
     + case when coalesce(nin_reuse.beneficiary_count, 0) > 1 then 1 else 0 end
     + case when coalesce(phone_reuse.beneficiary_count, 0) > 1 then 1 else 0 end
     + case when coalesce(loans.loan_count, 0) > 1 then 1 else 0 end
     + case when coalesce(substitution.account_substitution_count, 0) > 0 then 1 else 0 end
     + case when coalesce(substitution.shared_account_payment_count, 0) > 0 then 1 else 0 end
    ) as identity_alert_count,
    case
        when coalesce(substitution.account_substitution_count, 0) > 0
          or coalesce(nin_reuse.beneficiary_count, 0) > 1 then 'HIGH'
        when not coalesce(identity.nin_verified, false)
          or coalesce(phone_reuse.beneficiary_count, 0) > 1
          or coalesce(loans.loan_count, 0) > 1
          or coalesce(substitution.shared_account_payment_count, 0) > 0 then 'MEDIUM'
        else 'LOW'
    end as identity_risk_band,
    concat_ws('; ',
        case when not coalesce(identity.nin_verified, false) then 'NIN_UNVERIFIED' end,
        case when coalesce(nin_reuse.beneficiary_count, 0) > 1 then 'SHARED_IDENTITY' end,
        case when coalesce(phone_reuse.beneficiary_count, 0) > 1 then 'SHARED_PHONE_TOKEN' end,
        case when coalesce(loans.loan_count, 0) > 1 then 'MULTIPLE_LOANS' end,
        case when coalesce(substitution.account_substitution_count, 0) > 0
            then 'ACCOUNT_SUBSTITUTION' end,
        case when coalesce(substitution.shared_account_payment_count, 0) > 0
            then 'SHARED_ACCOUNT_TOKEN' end
    ) as identity_alert_reason,
    case
        when coalesce(substitution.account_substitution_count, 0) > 0
          or coalesce(nin_reuse.beneficiary_count, 0) > 1 then 'HIGH'
        when not coalesce(identity.nin_verified, false)
          or coalesce(phone_reuse.beneficiary_count, 0) > 1
          or coalesce(loans.loan_count, 0) > 1
          or coalesce(substitution.shared_account_payment_count, 0) > 0 then 'MEDIUM'
        else 'LOW'
    end as identity_alert_severity,
    (not coalesce(identity.nin_verified, false)
      or coalesce(nin_reuse.beneficiary_count, 0) > 1
      or coalesce(phone_reuse.beneficiary_count, 0) > 1
      or coalesce(substitution.account_substitution_count, 0) > 0
      or coalesce(substitution.shared_account_payment_count, 0) > 0) as requires_investigation,
    'INDICATOR_NOT_FRAUD_DETERMINATION' as interpretation
from identity
left join nin_reuse using (nin_hashed)
left join phone_reuse using (phone_hashed)
left join loans using (beneficiary_id)
left join account_substitution substitution
  on identity.beneficiary_token = substitution.beneficiary_token
where identity.beneficiary_id is not null
