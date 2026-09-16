{{ config(
    materialized='iceberg_table',
    tags=['silver_vault', 'identity', 'signals'],
    meta={'classification': 'internal'}
) }}

-- Pseudonymous per-token identity-signal projection of the RESTRICTED
-- identity-alert product. No direct identifiers, no correlator hashes, no
-- clear geography: only alert counts, bands and booleans keyed by the
-- canonical beneficiary_token so ordinary layers can consume alert SIGNALS
-- without any access to the identity vault's clear context.

select
    beneficiary_token,
    token_version,
    beneficiary_sk,
    account_substitution_count,
    account_substitution_amount,
    identity_alert_count,
    identity_risk_band,
    identity_alert_severity,
    has_unverified_nin,
    has_shared_nin,
    has_shared_phone,
    has_multiple_loans,
    has_account_substitution,
    has_shared_account,
    requires_investigation,
    interpretation
from {{ ref('vlt_pdm_beneficiary_identity_alerts') }}
