{{ config(materialized='iceberg_table', tags=['consumption', 'local-government']) }}

select
    parish.parish_sk,
    parish.region,
    parish.district,
    parish.parish,
    parish.loan_count,
    parish.beneficiary_count,
    parish.approved_amount,
    parish.disbursed_amount,
    parish.repaid_amount,
    parish.outstanding_amount,
    parish.disbursement_rate,
    parish.principal_repayment_rate,
    risk.high_identity_alert_count,
    risk.account_substitution_amount,
    risk.disbursement_peer_zscore,
    risk.repayment_peer_zscore,
    risk.geographic_risk_band,
    case when risk.geographic_risk_band = 'HIGH' then 'CRITICAL'
         when risk.geographic_risk_band = 'MEDIUM' then 'AT_RISK'
         else 'ON_TRACK' end as local_performance_status
from {{ ref('cns_pdm_parish_performance') }} parish
left join {{ ref('cns_pdm_geographic_risk') }} risk using (parish_sk)
