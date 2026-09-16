{{ config(
    materialized='iceberg_table',
    tags=['silver_vault', 'identity', 'restricted'],
    meta={'classification': 'restricted'}
) }}

-- RESTRICTED IDENTITY VAULT (silver_vault layer only).
-- Clear identity, direct identifiers, legacy token provenance and the approved
-- age bands for the canonical beneficiary population. This product is the only
-- ordinary layer where DIRECT_IDENTIFIER fields (per
-- platform/config/governance/data_classification_part1.yaml) may appear, and it
-- must never be readable by analyst / BI dashboard / agent / Gold / Consumption.
-- Ordinary Silver projects only the pseudonymous boundary attributes.

with bronze_dedup as (

    select
        beneficiary_id, beneficiary_token, nin, nin_hashed, nin_verified,
        beneficiary_name, date_of_birth, gender, phone, alternative_phone, email,
        phone_verified, household_id, special_group_code, village, parish,
        sub_county, county, district, region, registration_date, is_active,
        created_at, updated_at
    from {{ ref('br_pdm_pdmis_beneficiaries') }}
    qualify row_number() over (
        partition by beneficiary_id order by updated_at desc nulls last, kafka_timestamp desc, kafka_offset desc
    ) = 1

)

select
    b.beneficiary_id,
    link.beneficiary_key_internal,
    link.token_version,
    link.beneficiary_token,
    b.beneficiary_token as legacy_source_beneficiary_token,
    b.beneficiary_name,
    b.nin,
    case when nullif(trim(b.nin), '') is null then null
        else nullif(b.nin_hashed, sha256('')) end as nin_hashed,
    b.nin_verified,
    b.date_of_birth,
    case
        when b.date_of_birth is null then 'UNKNOWN'
        when date_diff('year', cast(b.date_of_birth as date), current_date) < 25 then '18-24'
        when date_diff('year', cast(b.date_of_birth as date), current_date) < 35 then '25-34'
        when date_diff('year', cast(b.date_of_birth as date), current_date) < 45 then '35-44'
        when date_diff('year', cast(b.date_of_birth as date), current_date) < 55 then '45-54'
        when date_diff('year', cast(b.date_of_birth as date), current_date) < 65 then '55-64'
        else '65+'
    end as age_band,
    b.gender,
    b.phone,
    -- Restricted reuse correlator; never projected into ordinary analytics.
    case when nullif(regexp_replace(coalesce(b.phone, ''), '[^0-9]', ''), '') is null then null
        else sha256(regexp_replace(b.phone, '[^0-9]', '')) end as phone_hashed,
    b.alternative_phone,
    b.email,
    b.phone_verified,
    b.household_id,
    b.special_group_code,
    b.village,
    b.parish,
    b.sub_county,
    b.county,
    b.district,
    b.region,
    b.registration_date,
    b.is_active,
    b.created_at,
    b.updated_at
from bronze_dedup b
inner join {{ source('silver_vault', 'vlt_pdm_beneficiary_token_link') }} link
    on b.beneficiary_id = link.beneficiary_id
where link.is_active
