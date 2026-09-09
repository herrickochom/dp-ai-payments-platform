{{ config(materialized='iceberg_table', tags=['consumption', 'performance']) }}

select
    {{ gold_surrogate_key(['geography.region', 'geography.district', 'geography.parish']) }} as parish_sk,
    geography.region, geography.district, geography.parish,
    count(distinct loan.loan_id) as loan_count,
    count(distinct loan.beneficiary_sk) as beneficiary_count,
    sum(loan.amount_approved) as approved_amount,
    sum(loan.amount_disbursed) as disbursed_amount,
    sum(loan.amount_repaid) as repaid_amount,
    sum(loan.principal_repaid) as principal_repaid_amount,
    sum(loan.outstanding_amount) as outstanding_amount,
    sum(loan.amount_disbursed) / nullif(sum(loan.amount_approved), 0) as disbursement_rate,
    -- Principal-based repayment: principal repaid against principal disbursed
    -- (PDM POC v1 decisions 1-2). amount_repaid is the total contractual repaid
    -- and includes interest, so it must never be used as the numerator here.
    sum(loan.principal_repaid) / nullif(sum(loan.amount_disbursed), 0) as principal_repayment_rate
from {{ ref('gld_fct_pdm_loans') }} loan
left join {{ ref('gld_dim_pdm_geography') }} geography using (geography_sk)
group by 1, 2, 3, 4
