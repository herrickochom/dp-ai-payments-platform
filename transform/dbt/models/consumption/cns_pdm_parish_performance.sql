{{ config(materialized='iceberg_table', tags=['consumption', 'performance']) }}

select
    loan.geography_sk, geography.region, geography.district, geography.parish,
    count(distinct loan.loan_id) as loan_count,
    count(distinct loan.beneficiary_sk) as beneficiary_count,
    sum(loan.amount_approved) as approved_amount,
    sum(loan.amount_disbursed) as disbursed_amount,
    sum(loan.amount_repaid) as repaid_amount,
    sum(loan.outstanding_amount) as outstanding_amount,
    sum(loan.amount_disbursed) / nullif(sum(loan.amount_approved), 0) as disbursement_rate,
    sum(loan.amount_repaid) / nullif(sum(loan.amount_disbursed), 0) as principal_repayment_rate
from {{ ref('fct_pdm_loans') }} loan
left join {{ ref('dim_pdm_geography') }} geography using (geography_sk)
group by 1, 2, 3, 4
