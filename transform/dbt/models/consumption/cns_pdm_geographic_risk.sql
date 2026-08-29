{{ config(materialized='iceberg_table', tags=['consumption', 'risk', 'geography']) }}

with identity_alerts as (
    select geography_sk,
        count(*) filter (where identity_risk_band = 'HIGH') as high_identity_alert_count,
        sum(account_substitution_amount) as account_substitution_amount
    from {{ ref('cns_pdm_beneficiary_identity_alerts') }}
    group by 1
), parish_metrics as (
    select
        parish.*,
        (parish.disbursement_rate - avg(parish.disbursement_rate) over (partition by parish.region))
            / nullif(stddev_pop(parish.disbursement_rate) over (partition by parish.region), 0) as disbursement_peer_zscore,
        (parish.principal_repayment_rate - avg(parish.principal_repayment_rate) over (partition by parish.region))
            / nullif(stddev_pop(parish.principal_repayment_rate) over (partition by parish.region), 0) as repayment_peer_zscore
    from {{ ref('cns_pdm_parish_performance') }} parish
)
select
    parish.*,
    coalesce(identity.high_identity_alert_count, 0) as high_identity_alert_count,
    coalesce(identity.account_substitution_amount, 0) as account_substitution_amount,
    case
        when coalesce(identity.high_identity_alert_count, 0) > 0
          or abs(coalesce(parish.disbursement_peer_zscore, 0)) >= 2
          or coalesce(parish.repayment_peer_zscore, 0) <= -2 then 'HIGH'
        when abs(coalesce(parish.disbursement_peer_zscore, 0)) >= 1
          or coalesce(parish.repayment_peer_zscore, 0) <= -1 then 'MEDIUM'
        else 'LOW'
    end as geographic_risk_band
from parish_metrics parish
left join identity_alerts identity using (geography_sk)
