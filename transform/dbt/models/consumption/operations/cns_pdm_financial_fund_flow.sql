{{ config(materialized='iceberg_table', tags=['consumption', 'finance', 'operations']) }}

-- Grain: one row per loan lifecycle. This intentionally derives directly
-- from the common Gold fact rather than an Investigator consumption model.
select
    lifecycle.lifecycle_sk,
    lifecycle.loan_id,
    lifecycle.beneficiary_sk,
    lifecycle.sacco_sk,
    lifecycle.geography_sk,
    lifecycle.amount_approved as approved_amount,
    lifecycle.instructed_amount,
    lifecycle.settled_amount,
    lifecycle.credited_amount,
    lifecycle.cashout_amount,
    lifecycle.amount_repaid as repaid_amount,
    lifecycle.approved_to_instructed_variance as approved_to_instruction_variance,
    lifecycle.instructed_to_settled_variance as instruction_to_settlement_variance,
    lifecycle.settled_to_credited_variance as settlement_to_credit_variance,
    lifecycle.credited_to_cashout_variance as credit_to_cashout_variance,
    lifecycle.disbursed_to_credited_variance,
    coalesce(lifecycle.amount_repaid, 0) - coalesce(lifecycle.amount_approved, 0)
        as approval_to_repayment_variance,
    lifecycle.approved_control_status,
    lifecycle.instructed_control_status,
    lifecycle.sent_status_control_status as accepted_control_status,
    lifecycle.settled_control_status,
    lifecycle.credited_control_status,
    lifecycle.cashout_control_status,
    lifecycle.repaid_control_status,
    cast(null as double) as recovered_amount,
    lifecycle.recovery_control_status,
    case
        when coalesce(lifecycle.approved_to_instructed_variance <> 0, false)
          or coalesce(lifecycle.instructed_to_settled_variance <> 0, false)
          or coalesce(lifecycle.settled_to_credited_variance <> 0, false)
          or coalesce(lifecycle.disbursed_to_credited_variance <> 0, false)
          or coalesce(lifecycle.credited_to_cashout_variance > 0, false)
          or (lifecycle.rejected_status_count > 0 and lifecycle.instruction_count > 1)
          or lifecycle.settlement_channel_count > 1
          or lifecycle.cashout_count > 1
          or (lifecycle.rejected_status_count > 0 and lifecycle.credit_notification_count > 0)
            then 'HIGH'
        when lifecycle.instructed_control_status = 'MISSING'
          or lifecycle.settled_control_status = 'MISSING'
          or lifecycle.credited_control_status = 'MISSING' then 'MEDIUM'
        else 'LOW'
    end as intervention_priority
from {{ ref('gld_fct_pdm_payment_lifecycle') }} lifecycle
