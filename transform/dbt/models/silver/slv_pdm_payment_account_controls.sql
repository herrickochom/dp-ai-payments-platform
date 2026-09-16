{{ config(materialized='iceberg_table', tags=['silver', 'privacy-boundary']) }}

-- Payment technical control signals at source-system/transaction grain.
-- Computes the account-substitution control ( creditor account vs the paying
-- beneficiary's contact number) entirely at the Silver boundary where the
-- restricted inputs are available, and projects ONLY technical correlation
-- keys, the account issuer and the boolean outcome: no account numbers and no
-- contact numbers are exposed. This preserves legitimate payment technical
-- semantics (is_account_substituted, creditor_account_issuer) for Gold facts
-- while the plain SHA-256 creditor-account hashing leaves ordinary Gold.

with creditors as (
    select
        t.source_system,
        t.transaction_id,
        t.end_to_end_id,
        pr.account_issuer,
        regexp_replace(pr.account_id, '[^0-9]', '') as creditor_account_digits
    from {{ ref('slv_pdm_payments_transactions') }} t
    left join {{ ref('slv_pdm_payment_party_roles') }} pr
        on t.message_id = pr.message_id and pr.party_role = 'CREDITOR'
    where t.transaction_id is not null
),

beneficiary_contact as (
    select
        loan_link.loan_id,
        identity.phone
    from {{ ref('slv_pdm_loan_beneficiary_links') }} loan_link
    inner join {{ ref('vlt_pdm_beneficiary_identity') }} identity
        on loan_link.beneficiary_token = identity.beneficiary_token
)

select
    creditors.source_system,
    creditors.transaction_id,
    creditors.account_issuer as creditor_account_issuer,
    case
        when creditors.account_issuer in ('MTN', 'AIRTEL')
             and nullif(creditors.creditor_account_digits, '') is not null
             and nullif(regexp_replace(coalesce(beneficiary_contact.phone, ''), '[^0-9]', ''), '')
                 is not null
             and creditors.creditor_account_digits
                 <> regexp_replace(beneficiary_contact.phone, '[^0-9]', '')
            then true
        when creditors.account_issuer is null
             or nullif(creditors.creditor_account_digits, '') is null
            then null
        else false
    end as is_account_substituted
from creditors
left join beneficiary_contact
    on creditors.end_to_end_id = beneficiary_contact.loan_id
