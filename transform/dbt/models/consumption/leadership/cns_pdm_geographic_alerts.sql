{{ config(materialized='iceberg_table', tags=['consumption', 'risk', 'geography', 'alerts', 'dashboard']) }}

with base as (
    select * from {{ ref('cns_pdm_parish_geographic_risk') }}
)

select
    region,
    district,
    parish,
    parish_sk,
    superset_district_iso,
    geographic_risk_band,
    geographic_risk_score,

    case
        when geographic_risk_band = 'SEVERE' then 1
        when geographic_risk_band = 'HIGH' then 2
        when geographic_risk_band = 'MEDIUM' then 3
        when geographic_risk_band = 'LOW' then 4
        else 5
    end as alert_priority,

    case
        when high_identity_alert_count >= 3 then 'REPEATED_HIGH_IDENTITY_ALERTS'
        when coalesce(repayment_peer_zscore, 0) <= -3 then 'SEVERE_REPAYMENT_UNDERPERFORMANCE'
        when abs(coalesce(disbursement_peer_zscore, 0)) >= 3 then 'SEVERE_DISBURSEMENT_OUTLIER'
        when high_identity_alert_count > 0 then 'IDENTITY_ALERT'
        when coalesce(repayment_peer_zscore, 0) <= -2 then 'HIGH_REPAYMENT_UNDERPERFORMANCE'
        when abs(coalesce(disbursement_peer_zscore, 0)) >= 2 then 'HIGH_DISBURSEMENT_OUTLIER'
        when geographic_risk_score is null then 'NO_DATA'
        else 'MONITOR'
    end as alert_type,

    disbursement_rate,
    principal_repayment_rate,
    disbursement_peer_zscore,
    repayment_peer_zscore,
    high_identity_alert_count,
    account_substitution_amount,
    coordinate_source,
    latitude,
    longitude

from base
where geographic_risk_band in ('SEVERE', 'HIGH', 'MEDIUM', 'NO DATA')
