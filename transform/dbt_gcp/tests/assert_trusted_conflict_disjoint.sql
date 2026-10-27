SELECT DISTINCT
    t.event_id
FROM {{ ref('slv_payment_events_trusted') }} t
INNER JOIN {{ ref('slv_payment_event_conflicts') }} c
    USING (event_id)
