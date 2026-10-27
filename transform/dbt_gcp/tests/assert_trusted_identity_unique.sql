SELECT
    event_id
FROM {{ ref('slv_payment_events_trusted') }}
GROUP BY event_id
HAVING COUNT(*) != 1
