{{ config(materialized='iceberg_table', tags=['gold', 'privacy-boundary']) }}

-- GATE 2 PRIVACY BOUNDARY: pseudonymous Type-1 beneficiary dimension.
-- Keyed on the canonical beneficiary_token; projects only privacy-safe
-- analytical attributes (gender, approved age band, verification flags,
-- special group, approved coarse geography). Clear identity, the internal
-- identifier, household identifiers and fine-grained personal geography are
-- retained only in the RESTRICTED identity vault. The former plain SHA-256
-- NIN/phone hashing is retired from ordinary Gold entirely.

select
    {{ gold_surrogate_key(['beneficiary.beneficiary_token']) }} as beneficiary_sk,
    {{ gold_surrogate_key(['beneficiary.region', 'beneficiary.district', 'beneficiary.county', 'beneficiary.sub_county']) }} as geography_sk,
    {{ gold_surrogate_key(['beneficiary.special_group_code']) }} as special_group_sk,
    beneficiary.beneficiary_token,
    beneficiary.token_version,
    beneficiary.gender,
    beneficiary.age_band,
    beneficiary.nin_verified,
    beneficiary.phone_verified,
    beneficiary.special_group_code,
    beneficiary.region,
    beneficiary.district,
    beneficiary.county,
    beneficiary.sub_county,
    beneficiary.registration_date,
    beneficiary.is_active
from {{ ref('slv_pdm_beneficiaries') }} beneficiary
where beneficiary.beneficiary_token is not null
union all
select
    {{ gold_surrogate_key(["cast(null as varchar)"]) }},
    {{ gold_surrogate_key(["cast(null as varchar)", "cast(null as varchar)", "cast(null as varchar)", "cast(null as varchar)"]) }},
    {{ gold_surrogate_key(["cast(null as varchar)"]) }},
    null, null, 'Unknown', 'Unknown', null, null, null, null, null, null, null, null, false
