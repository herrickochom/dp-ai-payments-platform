{{ config(materialized='iceberg_table', tags=['consumption', 'risk', 'geography', 'subcounty', 'dashboard']) }}

-- Sub-county risk is attributed through SACCO office locations, which are the
-- only platform source carrying sub_county. Loans are attributed to the
-- sub-county of the SACCO that administers them. Beneficiary parishes cannot
-- be nested under sub-counties until a parish-to-sub-county mapping exists,
-- so the drill from sub-county to parish is filter-driven on region and
-- district rather than a strict parent-child rollup.
--
-- Loans whose SACCO cannot be resolved to an office location are retained in
-- an 'Unknown' bucket so sub-county loan totals remain reconcilable with the
-- national portfolio.

with sacco_subcounties as (
    select
        sacco.sacco_sk,
        coalesce(nullif(trim(office.region), ''), 'Unknown') as region,
        coalesce(nullif(trim(office.district), ''), 'Unknown') as district,
        coalesce(nullif(trim(office.sub_county), ''), 'Unknown') as sub_county
    from {{ ref('gld_dim_pdm_sacco') }} sacco
    join {{ ref('slv_pdm_saccos') }} office
      on sacco.sacco_id = office.sacco_id
    where sacco.sacco_id is not null

), loan_subcounty as (
    select
        coalesce(location.region, 'Unknown') as region,
        coalesce(location.district, 'Unknown') as district,
        coalesce(location.sub_county, 'Unknown') as sub_county,
        loan.sacco_sk,
        loan.loan_id,
        loan.beneficiary_sk,
        loan.amount_approved,
        loan.amount_disbursed,
        loan.amount_repaid,
        loan.outstanding_amount
    from {{ ref('gld_fct_pdm_loans') }} loan
    left join sacco_subcounties location
      on loan.sacco_sk = location.sacco_sk

), subcounty_metrics as (
    select
        {{ gold_surrogate_key(['region', 'district', 'sub_county']) }} as sub_county_sk,
        region,
        district,
        sub_county,
        count(distinct sacco_sk) as sacco_count,
        count(distinct loan_id) as loan_count,
        count(distinct beneficiary_sk) as beneficiary_count,
        sum(amount_approved) as approved_amount,
        sum(amount_disbursed) as disbursed_amount,
        sum(amount_repaid) as repaid_amount,
        sum(outstanding_amount) as outstanding_amount,
        sum(amount_disbursed) / nullif(sum(amount_approved), 0) as disbursement_rate,
        sum(amount_repaid) / nullif(sum(amount_disbursed), 0) as principal_repayment_rate
    from loan_subcounty
    group by region, district, sub_county

), peer_zscores as (
    select
        metrics.*,
        (
            metrics.disbursement_rate
            - avg(metrics.disbursement_rate) over (partition by metrics.region)
        )
        / nullif(
            stddev_pop(metrics.disbursement_rate) over (partition by metrics.region),
            0
        ) as disbursement_peer_zscore,
        (
            metrics.principal_repayment_rate
            - avg(metrics.principal_repayment_rate) over (partition by metrics.region)
        )
        / nullif(
            stddev_pop(metrics.principal_repayment_rate) over (partition by metrics.region),
            0
        ) as repayment_peer_zscore
    from subcounty_metrics metrics

), beneficiary_subcounties as (
    -- Deduplicated beneficiary-to-sub-county attribution paths so a
    -- beneficiary with several loans through the same sub-county is
    -- counted once there.
    select distinct
        beneficiary_sk,
        region,
        district,
        sub_county
    from loan_subcounty
    where beneficiary_sk is not null

), identity_alerts as (
    select
        path.region,
        path.district,
        path.sub_county,
        count(*) filter (where alert.identity_risk_band = 'HIGH') as high_identity_alert_count,
        sum(alert.account_substitution_amount) as account_substitution_amount
    from {{ ref('cns_pdm_beneficiary_identity_alerts') }} alert
    join beneficiary_subcounties path
      on alert.beneficiary_sk = path.beneficiary_sk
    group by 1, 2, 3

), district_iso_map as (
    select
        lower(trim(district)) as district_key,
        superset_district_iso
    from {{ ref('uganda_superset_district_iso') }}

), scored as (
    select
        peers.*,
        map.superset_district_iso,
        case
            when map.superset_district_iso is null then 'UNMAPPED'
            else 'MAPPED'
        end as map_mapping_status,
        district_geo.latitude,
        district_geo.longitude,
        case
            when district_geo.latitude is not null then 'DISTRICT_CENTROID'
            else 'UNMAPPED'
        end as coordinate_source,
        coalesce(identity.high_identity_alert_count, 0) as high_identity_alert_count,
        coalesce(identity.account_substitution_amount, 0) as account_substitution_amount
    from peer_zscores peers
    left join identity_alerts identity
      on peers.region = identity.region
     and peers.district = identity.district
     and peers.sub_county = identity.sub_county
    left join district_iso_map map
      on lower(trim(peers.district)) = map.district_key
    left join {{ ref('cns_pdm_district_geographic_risk') }} district_geo
      on peers.region = district_geo.region
     and peers.district = district_geo.district
)

select
    scored.*,
    case
        -- Sub-county rows are not map-driven, so unlike the parish and
        -- district models the band is not gated on Superset district ISO
        -- availability. NO DATA still applies when no risk signal exists.
        when disbursement_peer_zscore is null
         and repayment_peer_zscore is null
         and high_identity_alert_count = 0 then 'NO DATA'

        when high_identity_alert_count >= 3
          or abs(coalesce(disbursement_peer_zscore, 0)) >= 3
          or coalesce(repayment_peer_zscore, 0) <= -3 then 'SEVERE'

        when high_identity_alert_count > 0
          or abs(coalesce(disbursement_peer_zscore, 0)) >= 2
          or coalesce(repayment_peer_zscore, 0) <= -2 then 'HIGH'

        when abs(coalesce(disbursement_peer_zscore, 0)) >= 1
          or coalesce(repayment_peer_zscore, 0) <= -1 then 'MEDIUM'

        else 'LOW'
    end as geographic_risk_band,

    case
        when disbursement_peer_zscore is null
         and repayment_peer_zscore is null
         and high_identity_alert_count = 0 then null

        when high_identity_alert_count >= 3
          or abs(coalesce(disbursement_peer_zscore, 0)) >= 3
          or coalesce(repayment_peer_zscore, 0) <= -3 then 4

        when high_identity_alert_count > 0
          or abs(coalesce(disbursement_peer_zscore, 0)) >= 2
          or coalesce(repayment_peer_zscore, 0) <= -2 then 3

        when abs(coalesce(disbursement_peer_zscore, 0)) >= 1
          or coalesce(repayment_peer_zscore, 0) <= -1 then 2

        else 1
    end as geographic_risk_score

from scored
