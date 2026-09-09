{{ config(
    materialized='iceberg_table',
    tags=[
        'consumption',
        'leadership',
        'risk',
        'geography',
        'district',
        'geojson',
        'superset',
        'dashboard'
    ]
) }}

-- ============================================================================
-- PDM DISTRICT GEOJSON RISK
-- ============================================================================
--
-- Purpose:
--   Presentation-ready district geography dataset for Superset Deck.gl GeoJSON.
--
-- Grain:
--   One row per Uganda district.
--
-- Geometry:
--   GeoJSON Feature stored in the `geojson` column.
--
-- Risk scale:
--   0 = NO DATA
--   1 = LOW
--   2 = MEDIUM
--   3 = HIGH
--   4 = SEVERE
--
-- Superset usage:
--
--   Chart:
--       deck.gl GeoJson
--
--   GeoJson Column:
--       geojson
--
--   Tooltip:
--       district
--       geographic_risk_band
--       beneficiary_count
--       loan_count
--       disbursed_amount
--       outstanding_amount
--
--   Label property name:
--       district
--
-- ============================================================================


with district_risk as (

    select
        region,
        district,
        superset_district_iso,

        map_mapping_status,
        district_data_status,

        parish_count,
        assessed_parish_count,

        severe_parish_count,
        high_parish_count,
        medium_parish_count,
        low_parish_count,

        loan_count,
        beneficiary_count,

        approved_amount,
        disbursed_amount,
        repaid_amount,
        outstanding_amount,

        geographic_risk_score,
        map_risk_score,
        geographic_risk_band,
        geographic_risk_sort_order,
        geographic_risk_legend_label,

        avg_disbursement_rate,
        avg_principal_repayment_rate,
        avg_disbursement_peer_zscore,
        avg_repayment_peer_zscore,

        high_identity_alert_count,
        account_substitution_amount,
        mapped_agent_count,

        latitude,
        longitude

    from {{ ref('cns_pdm_district_geographic_risk') }}

),


district_geometry as (

    select
        trim(district) as district,
        lower(trim(district)) as district_key,

        trim(superset_district_iso) as superset_district_iso,

        geojson_geometry

    from {{ ref('uganda_district_geojson') }}

),


joined as (

    select
        risk.region,

        risk.district,
        risk.superset_district_iso,

        risk.map_mapping_status,
        risk.district_data_status,

        risk.parish_count,
        risk.assessed_parish_count,

        risk.severe_parish_count,
        risk.high_parish_count,
        risk.medium_parish_count,
        risk.low_parish_count,

        risk.loan_count,
        risk.beneficiary_count,

        risk.approved_amount,
        risk.disbursed_amount,
        risk.repaid_amount,
        risk.outstanding_amount,

        risk.geographic_risk_score,
        risk.map_risk_score,
        risk.geographic_risk_band,
        risk.geographic_risk_sort_order,
        risk.geographic_risk_legend_label,

        risk.avg_disbursement_rate,
        risk.avg_principal_repayment_rate,
        risk.avg_disbursement_peer_zscore,
        risk.avg_repayment_peer_zscore,

        risk.high_identity_alert_count,
        risk.account_substitution_amount,
        risk.mapped_agent_count,

        risk.latitude,
        risk.longitude,

        geometry.geojson_geometry

    from district_risk risk

    left join district_geometry geometry
      on lower(trim(risk.district))
       = geometry.district_key

),


final as (

    select
        region,

        district,
        superset_district_iso,

        map_mapping_status,

        case
            when geojson_geometry is null then 'NO GEOMETRY'
            else 'HAS GEOMETRY'
        end as geometry_status,

        district_data_status,

        parish_count,
        assessed_parish_count,

        severe_parish_count,
        high_parish_count,
        medium_parish_count,
        low_parish_count,

        loan_count,
        beneficiary_count,

        approved_amount,
        disbursed_amount,
        repaid_amount,
        outstanding_amount,

        geographic_risk_score,
        map_risk_score,
        geographic_risk_band,
        geographic_risk_sort_order,
        geographic_risk_legend_label,

        avg_disbursement_rate,
        avg_principal_repayment_rate,
        avg_disbursement_peer_zscore,
        avg_repayment_peer_zscore,

        high_identity_alert_count,
        account_substitution_amount,
        mapped_agent_count,

        latitude,
        longitude,

        -- --------------------------------------------------------------------
        -- Superset Deck.gl GeoJSON Feature
        --
        -- The geometry seed stores only the GeoJSON geometry object:
        --
        -- {
        --   "type": "Polygon",
        --   "coordinates": [...]
        -- }
        --
        -- Here we turn it into a full GeoJSON Feature and inject PDM properties
        -- that Deck.gl can use for labels/tooltips/JS styling.
        -- --------------------------------------------------------------------

        case
            when geojson_geometry is null then null

            else concat(
                '{',
                    '"type":"Feature",',

                    '"properties":{',

                        '"district":"',
                        replace(
                            coalesce(district, ''),
                            '"',
                            '\\"'
                        ),
                        '",',

                        '"iso_code":"',
                        replace(
                            coalesce(superset_district_iso, ''),
                            '"',
                            '\\"'
                        ),
                        '",',

                        '"region":"',
                        replace(
                            coalesce(region, ''),
                            '"',
                            '\\"'
                        ),
                        '",',

                        '"risk_band":"',
                        replace(
                            coalesce(
                                geographic_risk_band,
                                'NO DATA'
                            ),
                            '"',
                            '\\"'
                        ),
                        '",',

                        '"risk_score":',
                        cast(
                            coalesce(
                                map_risk_score,
                                0
                            )
                            as varchar
                        ),

                    '},',

                    '"geometry":',
                    geojson_geometry,

                '}'
            )
        end as geojson

    from joined

)


select *
from final
order by district
