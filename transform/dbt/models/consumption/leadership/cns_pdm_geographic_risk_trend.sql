{{ config(materialized='iceberg_table', tags=['consumption', 'risk', 'geography', 'trend', 'dashboard']) }}

-- Current-state daily snapshot model.
-- With iceberg_table materialization this is the latest snapshot only.
-- If you later introduce an incremental Iceberg snapshot materialization,
-- retain snapshot_date as part of the unique grain to build true history.

select
    current_date as snapshot_date,
    region,
    district,
    superset_district_iso,
    geographic_risk_band,
    geographic_risk_score,
    parish_count,
    assessed_parish_count,
    severe_parish_count,
    high_parish_count,
    medium_parish_count,
    low_parish_count,
    avg_disbursement_rate,
    avg_principal_repayment_rate,
    high_identity_alert_count,
    account_substitution_amount,
    mapped_agent_count
from {{ ref('cns_pdm_district_geographic_risk') }}
