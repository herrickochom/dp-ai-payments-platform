{{ config(materialized='iceberg_table') }}

with payment_stages as (
    select
        end_to_end_id as loan_id,
        count(*) filter (where source_system in ('ICMN_VPM', 'WENDI_PAIN001')) as instruction_count,
        max(amount) filter (where source_system in ('ICMN_VPM', 'WENDI_PAIN001')) as instructed_amount,
        count(*) filter (where source_system in ('MTN_PACS008', 'AIRTEL_PACS008')) as settlement_count,
        count(distinct source_system) filter (where source_system in ('MTN_PACS008', 'AIRTEL_PACS008')) as settlement_channel_count,
        max(amount) filter (where source_system in ('MTN_PACS008', 'AIRTEL_PACS008')) as settled_amount,
        count(*) filter (where source_system = 'AGENT') as cashout_count,
        sum(amount) filter (where source_system = 'AGENT') as cashout_amount,
        min(occurred_at) filter (where source_system = 'AGENT') as first_cashout_at
    from {{ ref('slv_pdm_payments_transactions') }}
    where end_to_end_id is not null
    group by 1
), statuses as (
    select
        end_to_end_id as loan_id,
        count(*) filter (where status_source in ('CPO_PSN', 'CPO_PLM', 'WENDI')) as payment_status_count,
        count(*) filter (where status_source in ('MTN', 'AIRTEL')) as settlement_status_count,
        count(*) filter (where transaction_status = 'RJCT' or group_status = 'RJCT') as rejected_status_count,
        count(*) filter (where transaction_status = 'ACSC' or group_status = 'ACSC') as accepted_status_count
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
    coalesce(statuses.payment_status_count, 0) as payment_status_count,
    coalesce(payment_stages.settlement_count, 0) as settlement_count,
    coalesce(payment_stages.settlement_channel_count, 0) as settlement_channel_count,
    payment_stages.settled_amount,
    coalesce(statuses.settlement_status_count, 0) as settlement_status_count,
    coalesce(credits.credit_notification_count, 0) as credit_notification_count,
    credits.credited_amount,
    credits.first_credited_at,
    coalesce(statements.statement_evidence_count, 0) as statement_evidence_count,
    coalesce(payment_stages.cashout_count, 0) as cashout_count,
    payment_stages.cashout_amount,
    payment_stages.first_cashout_at,
    coalesce(statuses.rejected_status_count, 0) as rejected_status_count,
    coalesce(statuses.accepted_status_count, 0) as accepted_status_count,
    case when loan.amount_approved is not null and loan.amount_approved > 0 then 'OBSERVED' else 'MISSING' end as approved_control_status,
    case when coalesce(payment_stages.instruction_count, 0) > 0 then 'OBSERVED' else 'MISSING' end as instructed_control_status,
    case when coalesce(statuses.payment_status_count, 0) > 0 then 'OBSERVED' else 'MISSING' end as sent_status_control_status,
    case when coalesce(payment_stages.settlement_count, 0) > 0 and coalesce(statuses.settlement_status_count, 0) > 0 then 'OBSERVED' else 'MISSING' end as settled_control_status,
    case when coalesce(credits.credit_notification_count, 0) > 0 then 'OBSERVED' else 'MISSING' end as credited_control_status,
    case when coalesce(payment_stages.cashout_count, 0) > 0 then 'OBSERVED' else 'MISSING' end as cashout_control_status,
    case when coalesce(loan.amount_repaid, 0) > 0 then 'OBSERVED' else 'MISSING' end as repaid_control_status,
    'NOT_OBSERVABLE' as recovery_control_status,
    'NOT_OBSERVABLE' as deceased_eligibility_control_status,
    'NOT_OBSERVABLE' as official_device_collusion_control_status,
    coalesce(payment_stages.instructed_amount, 0) - coalesce(loan.amount_approved, 0) as approved_to_instructed_variance,
    coalesce(payment_stages.settled_amount, 0) - coalesce(payment_stages.instructed_amount, 0) as instructed_to_settled_variance,
    coalesce(credits.credited_amount, 0) - coalesce(payment_stages.settled_amount, 0) as settled_to_credited_variance
from {{ ref('slv_pdm_loans') }} loan
left join payment_stages using (loan_id)
left join statuses using (loan_id)
left join credits using (loan_id)
left join statements using (loan_id)
