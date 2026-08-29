{{ config(materialized='iceberg_table') }}

select debtor_account_id as account_id, debtor_account_issuer as account_issuer,
       cast(null as varchar) as account_scheme
from {{ ref('br_pdm_icmn_vpm_pain001') }}
where debtor_account_id is not null

union

select creditor_account_id, creditor_account_issuer, creditor_account_scheme
from {{ ref('br_pdm_icmn_vpm_pain001') }}
where creditor_account_id is not null
