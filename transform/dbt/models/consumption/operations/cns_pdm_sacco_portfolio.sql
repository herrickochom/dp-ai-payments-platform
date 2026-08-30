{{ config(materialized='iceberg_table', tags=['consumption', 'sacco']) }}

select
    sacco.sacco_sk,
    sacco.geography_sk,
    sacco.sacco_id,
    sacco.sacco_name,
    sacco.office_exists,
    sacco.is_active,
    count(loan.loan_id) as loan_count,
    count(distinct loan.beneficiary_sk) as beneficiary_count,
    sum(loan.amount_requested) as requested_amount,
    sum(loan.amount_approved) as approved_amount,
    sum(loan.amount_disbursed) as disbursed_amount,
    sum(loan.amount_repaid) as repaid_amount,
    sum(loan.outstanding_amount) as outstanding_amount,
    sum(loan.undisbursed_amount) as undisbursed_amount,
    sum(loan.amount_repaid) / nullif(sum(loan.amount_disbursed), 0) as principal_repayment_rate,
    sum(loan.amount_disbursed) / nullif(sum(loan.amount_approved), 0) as disbursement_rate,
    count(*) filter (where loan.outstanding_amount > 0) as loans_with_outstanding_balance
from {{ ref('gld_dim_pdm_sacco') }} sacco
left join {{ ref('gld_fct_pdm_loans') }} loan using (sacco_sk)
where sacco.sacco_id is not null
group by 1, 2, 3, 4, 5, 6
