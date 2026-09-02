{{ config(materialized='iceberg_table', tags=['consumption', 'risk', 'geography', 'drivers', 'dashboard']) }}

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

    abs(coalesce(disbursement_peer_zscore, 0)) as disbursement_risk_magnitude,
    greatest(0, -coalesce(repayment_peer_zscore, 0)) as repayment_risk_magnitude,
    high_identity_alert_count as identity_risk_magnitude,

    case
        -- LOW parishes carry no material driver; a driver label there would
        -- overstate an on-track geography.
        when geographic_risk_score is null then 'NO DATA'
        when geographic_risk_band = 'LOW' then 'NONE'
        when high_identity_alert_count > 0
         and high_identity_alert_count >= abs(coalesce(disbursement_peer_zscore, 0))
         and high_identity_alert_count >= greatest(0, -coalesce(repayment_peer_zscore, 0))
            then 'IDENTITY'
        when greatest(0, -coalesce(repayment_peer_zscore, 0))
           >= abs(coalesce(disbursement_peer_zscore, 0))
            then 'REPAYMENT'
        else 'DISBURSEMENT'
    end as primary_risk_driver,

    disbursement_rate,
    principal_repayment_rate,
    disbursement_peer_zscore,
    repayment_peer_zscore,
    high_identity_alert_count,
    account_substitution_amount,
    mapped_agent_count,
    latitude,
    longitude

from base
