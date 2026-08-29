{{ config(materialized='iceberg_table') }}

select event_id, message_id, 'vpm' as payment_source, 'debtor' as party_role,
       debtor_name as party_name, debtor_account_id as account_id,
       debtor_agent_id as agent_id
from {{ ref('br_pdm_icmn_vpm_pain001') }}

union all

select event_id, message_id, 'vpm' as payment_source, 'creditor' as party_role,
       creditor_name as party_name, creditor_account_id as account_id,
       creditor_agent_id as agent_id
from {{ ref('br_pdm_icmn_vpm_pain001') }}
