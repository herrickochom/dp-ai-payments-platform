{{ config(
    materialized='iceberg_table',
    tags=['consumption', 'risk', 'identity', 'privacy-boundary'],
    meta={'classification': 'internal'}
) }}

-- GATE 2 PRIVACY BOUNDARY: this product is now a pseudonymous per-token
-- identity-theft indicator feed (alert counts, bands and booleans keyed by the
-- canonical beneficiary_token). The former clear-identity alert detail (direct
-- identifiers, identity hashes, fine-grained geography) has been RELOCATED to
-- the RESTRICTED identity vault product
-- (silver_vault.vlt_pdm_beneficiary_identity_alerts) and is not exposed to
-- ordinary consumption. Interpretation is unchanged:
-- INDICATOR_NOT_FRAUD_DETERMINATION.

select
    signals.beneficiary_token,
    signals.beneficiary_sk,
    signals.account_substitution_count,
    signals.account_substitution_amount,
    signals.identity_alert_count,
    signals.identity_risk_band,
    signals.identity_alert_severity,
    signals.has_unverified_nin,
    signals.has_shared_nin,
    signals.has_shared_phone,
    signals.has_multiple_loans,
    signals.has_account_substitution,
    signals.has_shared_account,
    signals.requires_investigation,
    signals.interpretation
from {{ ref('vlt_pdm_beneficiary_identity_signals') }} signals
