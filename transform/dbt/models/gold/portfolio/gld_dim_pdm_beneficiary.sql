{{ config(materialized='iceberg_table') }}

select
    {{ gold_surrogate_key(['beneficiary.beneficiary_id']) }} as beneficiary_sk,
    {{ gold_surrogate_key(['beneficiary.region', 'beneficiary.district', 'beneficiary.county', 'beneficiary.sub_county', 'beneficiary.parish', 'beneficiary.village']) }} as geography_sk,
    {{ gold_surrogate_key(['beneficiary.special_group_code']) }} as special_group_sk,
    beneficiary.beneficiary_id,
    beneficiary.beneficiary_token,
    case
        -- Derive nullity from the raw NIN: a missing/blank NIN must not inherit
        -- a source-side sentinel hash (e.g. sha256('')) that would make every
        -- unregistered beneficiary appear to share one identity.
        when nullif(trim(beneficiary.nin), '') is null then null
        else nullif(beneficiary.nin_hashed, sha256(''))
    end as nin_hashed,
    case
        when nullif(
            regexp_replace(coalesce(beneficiary.phone, ''), '[^0-9]', ''),
            ''
        ) is null then null
        else sha256(regexp_replace(beneficiary.phone, '[^0-9]', ''))
    end as phone_hashed,
    beneficiary.nin_verified,
    beneficiary.beneficiary_name,
    beneficiary.date_of_birth,
    beneficiary.gender,
    beneficiary.phone_verified,
    beneficiary.household_id,
    beneficiary.special_group_code,
    beneficiary.registration_date,
    beneficiary.is_active
from {{ ref('slv_pdm_beneficiaries') }} beneficiary
where beneficiary.beneficiary_id is not null
union all
select
    {{ gold_surrogate_key(["cast(null as varchar)"]) }},
    {{ gold_surrogate_key(["cast(null as varchar)", "cast(null as varchar)", "cast(null as varchar)", "cast(null as varchar)", "cast(null as varchar)", "cast(null as varchar)"]) }},
    {{ gold_surrogate_key(["cast(null as varchar)"]) }},
    null, null, null, null, null, 'Unknown', null, 'Unknown', null, null, null, null, false
