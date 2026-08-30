{{ config(materialized='iceberg_table', tags=['consumption', 'risk', 'reconciliation']) }}

select
    lifecycle.lifecycle_sk,
    lifecycle.loan_id,
    lifecycle.beneficiary_sk,
    lifecycle.sacco_sk,
    lifecycle.geography_sk,
    geography.region,
    geography.district,
    geography.parish,
    lifecycle.amount_approved,
    lifecycle.instructed_amount,
    lifecycle.settled_amount,
    lifecycle.credited_amount,
    lifecycle.cashout_amount,
    lifecycle.amount_repaid,
    lifecycle.approved_control_status,
    lifecycle.instructed_control_status,
    lifecycle.sent_status_control_status,
    lifecycle.settled_control_status,
    lifecycle.credited_control_status,
    lifecycle.cashout_control_status,
    lifecycle.repaid_control_status,
    lifecycle.recovery_control_status,
    lifecycle.deceased_eligibility_control_status,
    lifecycle.official_device_collusion_control_status,
    lifecycle.approved_to_instructed_variance,
    lifecycle.instructed_to_settled_variance,
    lifecycle.settled_to_credited_variance,
    lifecycle.disbursed_to_credited_variance,
    lifecycle.credited_to_cashout_variance,
    lifecycle.rejected_status_count,
    lifecycle.instruction_count,
    lifecycle.settlement_channel_count,
    lifecycle.cashout_count,
    lifecycle.rejected_status_count > 0 and lifecycle.instruction_count > 1
        as has_rejected_resubmission,
    lifecycle.settlement_channel_count > 1 as has_dual_settlement_route,
    lifecycle.cashout_count > 1 as has_multiple_cashouts,
    lifecycle.credited_to_cashout_variance > 0 as has_cashout_above_credit,
    lifecycle.rejected_status_count > 0
        and lifecycle.credit_notification_count > 0 as has_credit_after_rejection,
    case
        when lifecycle.approved_to_instructed_variance <> 0
          or lifecycle.instructed_to_settled_variance <> 0
          or lifecycle.settled_to_credited_variance <> 0
          or lifecycle.disbursed_to_credited_variance <> 0
          or lifecycle.credited_to_cashout_variance > 0
          or (lifecycle.rejected_status_count > 0 and lifecycle.instruction_count > 1)
          or lifecycle.settlement_channel_count > 1
          or lifecycle.cashout_count > 1
          or (lifecycle.rejected_status_count > 0
              and lifecycle.credit_notification_count > 0) then 'HIGH'
        when lifecycle.instructed_control_status = 'MISSING'
          or lifecycle.settled_control_status = 'MISSING'
          or lifecycle.credited_control_status = 'MISSING' then 'MEDIUM'
        else 'LOW'
    end as intervention_priority
from {{ ref('gld_fct_pdm_payment_lifecycle') }} lifecycle
left join {{ ref('gld_dim_pdm_geography') }} geography using (geography_sk)
