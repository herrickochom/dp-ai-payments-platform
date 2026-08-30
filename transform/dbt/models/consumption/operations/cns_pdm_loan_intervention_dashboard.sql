{{ config(materialized='iceberg_table', tags=['consumption', 'intervention']) }}

select
    lifecycle.lifecycle_sk,
    lifecycle.loan_id,
    lifecycle.beneficiary_sk,
    lifecycle.sacco_sk,
    lifecycle.geography_sk,
    lifecycle.region,
    lifecycle.district,
    lifecycle.parish,
    loan.loan_status,
    loan.project_type,
    lifecycle.amount_approved,
    loan.amount_disbursed,
    lifecycle.amount_repaid,
    loan.outstanding_amount,
    lifecycle.instructed_amount,
    lifecycle.settled_amount,
    lifecycle.credited_amount,
    lifecycle.cashout_amount,
    lifecycle.approved_control_status,
    lifecycle.instructed_control_status,
    lifecycle.sent_status_control_status,
    lifecycle.settled_control_status,
    lifecycle.credited_control_status,
    lifecycle.cashout_control_status,
    lifecycle.repaid_control_status,
    lifecycle.intervention_priority,
    lifecycle.has_rejected_resubmission,
    (case when lifecycle.instructed_control_status = 'MISSING' then 1 else 0 end
     + case when lifecycle.sent_status_control_status = 'MISSING' then 1 else 0 end
     + case when lifecycle.settled_control_status = 'MISSING' then 1 else 0 end
     + case when lifecycle.credited_control_status = 'MISSING' then 1 else 0 end
     + case when lifecycle.cashout_control_status = 'MISSING' then 1 else 0 end) as missing_stage_count,
    case when lifecycle.intervention_priority = 'LOW' then 'PROGRESSING'
         else 'REQUIRES_INTERVENTION' end as intervention_status
from {{ ref('cns_pdm_lifecycle_exceptions') }} lifecycle
left join {{ ref('gld_fct_pdm_loans') }} loan using (loan_id)
