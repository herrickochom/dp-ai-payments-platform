{{ config(materialized='iceberg_table', tags=['consumption', 'risk', 'reconciliation', 'privacy-boundary']) }}

-- GATE 2 PRIVACY BOUNDARY: pseudonymous by default (beneficiary_token from the
-- lifecycle fact; approved coarse geography only).

select
    lifecycle.lifecycle_sk,
    lifecycle.loan_id,
    lifecycle.beneficiary_sk,
    lifecycle.beneficiary_token,
    lifecycle.sacco_sk,
    sacco.sacco_id,
    lifecycle.geography_sk,
    lifecycle.region,
    lifecycle.district,
    lifecycle.county,
    lifecycle.sub_county,
    cast(lifecycle.approval_date as date) as approval_date,
    lifecycle.first_credited_at,
    lifecycle.first_cashout_at,
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
    coalesce(lifecycle.credited_to_cashout_variance > 0, false)
        as has_cashout_above_credit,
    lifecycle.rejected_status_count > 0
        and lifecycle.credit_notification_count > 0 as has_credit_after_rejection,
    case
        when coalesce(lifecycle.approved_to_instructed_variance <> 0, false)
          or coalesce(lifecycle.instructed_to_settled_variance <> 0, false)
          or coalesce(lifecycle.settled_to_credited_variance <> 0, false)
          or coalesce(lifecycle.disbursed_to_credited_variance <> 0, false)
          or coalesce(lifecycle.credited_to_cashout_variance > 0, false)
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
left join {{ ref('gld_dim_pdm_sacco') }} sacco using (sacco_sk)
