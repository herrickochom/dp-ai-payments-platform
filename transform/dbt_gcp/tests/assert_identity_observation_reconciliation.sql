WITH landing AS (
    SELECT COUNT(*) AS total_observations
    FROM {{ source('payments_landing', 'payment_events') }}
),

classified AS (
    SELECT
        COALESCE(SUM(observation_count), 0)
            AS total_observations
    FROM {{ ref('slv_payment_event_identity_status') }}
)

SELECT
    landing.total_observations AS landing_count,
    classified.total_observations AS classified_count
FROM landing
CROSS JOIN classified
WHERE landing.total_observations
   != classified.total_observations
