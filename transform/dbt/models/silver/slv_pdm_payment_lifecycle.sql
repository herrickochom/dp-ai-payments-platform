{{ config(materialized='iceberg_table') }}

with payment_stages as (
    select
        end_to_end_id as loan_id,
        count(*) filter (where source_system = 'ICMN_VPM') as instruction_count,
        max(amount) filter (where source_system = 'ICMN_VPM') as instructed_amount,
        count(*) filter (where source_system = 'WENDI_PAIN001') as routing_event_count,
        count(*) filter (where source_system = 'AGENT') as cashout_count,
        sum(amount) filter (where source_system = 'AGENT') as cashout_amount,
        min(occurred_at) filter (where source_system = 'AGENT') as first_cashout_at
    from {{ ref('slv_pdm_payments_transactions') }}
    where end_to_end_id is not null
    group by 1
), settlement_attempts as (
    select
        payment.end_to_end_id as loan_id,
        payment.source_system,
        count(*) as attempt_count,
        max(payment.amount) as attempted_amount,
        bool_or(status.transaction_status = 'ACSC' or status.group_status = 'ACSC')
            as was_accepted
    from {{ ref('slv_pdm_payments_transactions') }} payment
    left join {{ ref('slv_pdm_payments_status_report') }} status
      on payment.end_to_end_id = status.end_to_end_id
     and status.status_source = case payment.source_system
            when 'MTN_PACS008' then 'MTN'
            when 'AIRTEL_PACS008' then 'AIRTEL'
         end
    where payment.source_system in ('MTN_PACS008', 'AIRTEL_PACS008')
      and payment.end_to_end_id is not null
    group by 1, 2
), settlements as (
    select
        loan_id,
        sum(attempt_count) as settlement_attempt_count,
        count(*) as settlement_channel_count,
        count(*) filter (where was_accepted) as accepted_settlement_channel_count,
        count(*) filter (where was_accepted) as settlement_count,
        max(attempted_amount) filter (where was_accepted) as settled_amount
    from settlement_attempts
    group by 1
), statuses as (
    select
        end_to_end_id as loan_id,
        count(*) filter (where status_source in ('CPO_PSN', 'CPO_PLM', 'WENDI'))
            as payment_status_count,
        count(*) filter (where status_source in ('MTN', 'AIRTEL'))
            as settlement_status_count,
        count(*) filter (where transaction_status = 'RJCT' or group_status = 'RJCT')
            as rejected_status_count,
        count(*) filter (where transaction_status = 'PDNG' or group_status = 'PDNG')
            as pending_status_count,
        count(*) filter (where transaction_status = 'ACSC' or group_status = 'ACSC')
            as accepted_status_count
    from {{ ref('slv_pdm_payments_status_report') }}
    where end_to_end_id is not null
    group by 1
), credits as (
    select
        end_to_end_id as loan_id,
        count(*) as credit_notification_count,
        max(coalesce(transaction_amount, entry_amount)) as credited_amount,
        min(notification_created_at) as first_credited_at
    from {{ ref('br_pdm_wendi_camt054') }}
    where end_to_end_id is not null
    group by 1
), statements as (
    select end_to_end_id as loan_id, count(*) as statement_evidence_count
    from {{ ref('br_pdm_wendi_camt053') }}
    where end_to_end_id is not null
    group by 1
)
select
    loan.loan_id,
    loan.beneficiary_id,
    loan.sacco_id,
    loan.approval_date,
    loan.amount_approved,
    loan.amount_disbursed,
    loan.amount_repaid,
    coalesce(payment_stages.instruction_count, 0) as instruction_count,
    payment_stages.instructed_amount,
    coalesce(payment_stages.routing_event_count, 0) as routing_event_count,
    coalesce(statuses.payment_status_count, 0) as payment_status_count,
    coalesce(settlements.settlement_attempt_count, 0) as settlement_attempt_count,
    coalesce(settlements.settlement_count, 0) as settlement_count,
    coalesce(settlements.settlement_channel_count, 0) as settlement_channel_count,
    coalesce(settlements.accepted_settlement_channel_count, 0)
        as accepted_settlement_channel_count,
    settlements.settled_amount,
    coalesce(statuses.settlement_status_count, 0) as settlement_status_count,
    coalesce(credits.credit_notification_count, 0) as credit_notification_count,
    credits.credited_amount,
    credits.first_credited_at,
    coalesce(statements.statement_evidence_count, 0) as statement_evidence_count,
    coalesce(payment_stages.cashout_count, 0) as cashout_count,
    payment_stages.cashout_amount,
    payment_stages.first_cashout_at,
    coalesce(statuses.rejected_status_count, 0) as rejected_status_count,
    coalesce(statuses.pending_status_count, 0) as pending_status_count,
    coalesce(statuses.accepted_status_count, 0) as accepted_status_count,
    case when loan.amount_approved > 0 then 'OBSERVED' else 'MISSING' end
        as approved_control_status,
    case when coalesce(payment_stages.instruction_count, 0) > 0 then 'OBSERVED'
        else 'MISSING' end as instructed_control_status,
    case when coalesce(statuses.payment_status_count, 0) > 0 then 'OBSERVED'
        else 'MISSING' end as sent_status_control_status,
    case
        when coalesce(settlements.settlement_count, 0) > 0 then 'OBSERVED'
        when coalesce(statuses.rejected_status_count, 0) > 0 then 'REJECTED'
        when coalesce(statuses.pending_status_count, 0) > 0 then 'PENDING'
        else 'MISSING'
    end as settled_control_status,
    case
        when coalesce(credits.credit_notification_count, 0) > 0 then 'OBSERVED'
        when coalesce(statuses.rejected_status_count, 0) > 0 then 'NOT_APPLICABLE'
        when coalesce(statuses.pending_status_count, 0) > 0 then 'PENDING'
        else 'MISSING'
    end as credited_control_status,
    case
        when coalesce(payment_stages.cashout_count, 0) > 0 then 'OBSERVED'
        when coalesce(statuses.rejected_status_count, 0) > 0 then 'NOT_APPLICABLE'
        when coalesce(statuses.pending_status_count, 0) > 0 then 'PENDING'
        else 'MISSING'
    end as cashout_control_status,
    case when coalesce(loan.amount_repaid, 0) > 0 then 'OBSERVED'
        else 'MISSING' end as repaid_control_status,
    'NOT_OBSERVABLE' as recovery_control_status,
    'NOT_OBSERVABLE' as deceased_eligibility_control_status,
    'NOT_OBSERVABLE' as official_device_collusion_control_status,
    payment_stages.instructed_amount - loan.amount_approved
        as approved_to_instructed_variance,
    settlements.settled_amount - payment_stages.instructed_amount
        as instructed_to_settled_variance,
    credits.credited_amount - settlements.settled_amount
        as settled_to_credited_variance,
    credits.credited_amount - loan.amount_disbursed
        as disbursed_to_credited_variance,
    payment_stages.cashout_amount - credits.credited_amount
        as credited_to_cashout_variance
from {{ ref('slv_pdm_loans') }} loan
left join payment_stages using (loan_id)
left join settlements using (loan_id)
left join statuses using (loan_id)
left join credits using (loan_id)
left join statements using (loan_id)
