{{ config(materialized='iceberg_table', tags=['silver', 'privacy-boundary']) }}

-- GATE 2 PRIVACY BOUNDARY: ordinary Silver analytical projection of the
-- canonical beneficiary population. Pseudonymous by default:
--   * beneficiary_token (HMAC-SHA256, keyed, versioned) is the canonical
--     analytical identifier (Tier 3)
--   * direct identifiers (full name, national ID number, date of birth,
--     contact numbers, email), the clear internal identifier, household
--     identifiers and fine-grained personal geography (village, parish) live
--     ONLY in the RESTRICTED identity vault
--   * the legacy Bronze token (TOKEN-<16hex>, unkeyed source-identity hash) is
--     classified restricted provenance and is retained only in the vault as
--     legacy_source_beneficiary_token
--   * date of birth is consumed only inside the vault to derive the approved
--     age band projected here

select
    identity.beneficiary_token,
    identity.token_version,
    identity.gender,
    identity.age_band,
    identity.nin_verified,
    identity.phone_verified,
    identity.special_group_code,
    identity.sub_county,
    identity.county,
    identity.district,
    identity.region,
    identity.registration_date,
    identity.is_active
from {{ ref('vlt_pdm_beneficiary_identity') }} identity
where identity.beneficiary_token is not null
