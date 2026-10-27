WITH landing_identities AS (
    SELECT DISTINCT event_id
    FROM {{ source('payments_landing', 'payment_events') }}
),

classified_identities AS (
    SELECT event_id
    FROM {{ ref('slv_payment_event_identity_status') }}
),

missing AS (
    SELECT event_id FROM landing_identities
    EXCEPT DISTINCT
    SELECT event_id FROM classified_identities
),

unexpected AS (
    SELECT event_id FROM classified_identities
    EXCEPT DISTINCT
    SELECT event_id FROM landing_identities
)

SELECT * FROM missing
UNION ALL
SELECT * FROM unexpected
