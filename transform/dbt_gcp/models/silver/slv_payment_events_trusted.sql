{{ config(materialized='table') }}

WITH verified_identities AS (
    SELECT event_id
    FROM {{ ref('slv_payment_event_identity_status') }}
    WHERE identity_status = 'IDENTITY_VERIFIED'
),

verified_payloads AS (
    SELECT DISTINCT
        event_id,
        payment_id,
        event_type,
        amount_minor,
        currency,
        event_timestamp
    FROM {{ source('payments_landing', 'payment_events') }}
    WHERE event_id IN (
        SELECT event_id FROM verified_identities
    )
)

SELECT * FROM verified_payloads
