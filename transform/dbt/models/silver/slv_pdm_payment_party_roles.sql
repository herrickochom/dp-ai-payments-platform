{{ config(materialized='iceberg_table') }}

select message_id, 'DEBTOR' as party_role,
       debtor_account_id as account_id, debtor_account_issuer as account_issuer
from {{ ref('br_pdm_icmn_vpm_pain001') }}
where debtor_account_id is not null

union

select message_id, 'CREDITOR', creditor_account_id, creditor_account_issuer
from {{ ref('br_pdm_icmn_vpm_pain001') }}
where creditor_account_id is not null
