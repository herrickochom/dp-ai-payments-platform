{{ config(materialized='pdm_iceberg_guarded') }}

select debtor_account_id as account_id, debtor_account_issuer as account_issuer,
       debtor_name as party_name
from {{ ref('br_pdm_icmn_vpm_pain001') }}
where debtor_account_id is not null

UNION DISTINCT

select creditor_account_id, creditor_account_issuer, creditor_name
from {{ ref('br_pdm_icmn_vpm_pain001') }}
where creditor_account_id is not null
