{{ config(materialized='table') }}

WITH conflicted_identities AS (
    SELECT
        event_id,
        observation_count,
        distinct_payload_count
    FROM {{ ref('slv_payment_event_identity_status') }}
    WHERE identity_status = 'IDENTITY_CONFLICT'
),

conflict_variants AS (
    SELECT
        event_id,
        payment_id,
        event_type,
        amount_minor,
        currency,
        event_timestamp,
        COUNT(*) AS variant_observation_count
    FROM {{ source('payments_landing', 'payment_events') }}
    WHERE event_id IN (
        SELECT event_id FROM conflicted_identities
    )
    GROUP BY
        event_id,
        payment_id,
        event_type,
        amount_minor,
        currency,
        event_timestamp
)

SELECT
    v.*,
    i.observation_count,
    i.distinct_payload_count
FROM conflict_variants v
JOIN conflicted_identities i
    USING (event_id)
