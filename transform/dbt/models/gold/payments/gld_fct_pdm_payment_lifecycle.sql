{{ config(materialized='iceberg_table', tags=['gold', 'privacy-boundary']) }}

-- GATE 2 PRIVACY BOUNDARY: accumulating loan control fact keyed to the
-- canonical beneficiary_token via the pseudonymous loan linkage, carrying only
-- approved coarse geography from Silver.

select
    {{ gold_surrogate_key(['lifecycle.loan_id']) }} as lifecycle_sk,
    {{ gold_surrogate_key(['loan_link.beneficiary_token']) }} as beneficiary_sk,
    {{ gold_surrogate_key(['lifecycle.sacco_id']) }} as sacco_sk,
    {{ gold_surrogate_key(['beneficiary.region', 'beneficiary.district', 'beneficiary.county', 'beneficiary.sub_county']) }} as geography_sk,
    {{ gold_surrogate_key(['cast(lifecycle.approval_date as date)']) }} as approval_date_sk,
    lifecycle.loan_id,
    lifecycle.approval_date,
    lifecycle.amount_approved,
    lifecycle.amount_disbursed,
    lifecycle.amount_repaid,
    lifecycle.instruction_count,
    lifecycle.instructed_amount,
    lifecycle.routing_event_count,
    lifecycle.payment_status_count,
    lifecycle.settlement_attempt_count,
    lifecycle.settlement_count,
    lifecycle.settlement_channel_count,
    lifecycle.accepted_settlement_channel_count,
    lifecycle.settled_amount,
    lifecycle.settlement_status_count,
    lifecycle.credit_notification_count,
    lifecycle.credited_amount,
    lifecycle.first_credited_at,
    lifecycle.statement_evidence_count,
    lifecycle.cashout_count,
    lifecycle.cashout_amount,
    lifecycle.first_cashout_at,
    lifecycle.rejected_status_count,
    lifecycle.pending_status_count,
    lifecycle.accepted_status_count,
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
    loan_link.beneficiary_token,
    beneficiary.region,
    beneficiary.district,
    beneficiary.county,
    beneficiary.sub_county
from {{ ref('slv_pdm_payment_lifecycle') }} lifecycle
left join {{ ref('slv_pdm_loan_beneficiary_links') }} loan_link
    on lifecycle.loan_id = loan_link.loan_id
left join {{ ref('slv_pdm_beneficiaries') }} beneficiary
    on loan_link.beneficiary_token = beneficiary.beneficiary_token
