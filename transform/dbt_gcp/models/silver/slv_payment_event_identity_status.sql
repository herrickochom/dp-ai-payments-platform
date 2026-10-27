{{ config(materialized='table') }}

WITH payload_variants AS (

    SELECT
        event_id,
        payment_id,
        event_type,
        amount_minor,
        currency,
        event_timestamp,
        COUNT(*) AS variant_observation_count

    FROM {{ source('payments_landing', 'payment_events') }}

    GROUP BY
        event_id,
        payment_id,
        event_type,
        amount_minor,
        currency,
        event_timestamp

),

identity_summary AS (

    SELECT
        event_id,
        SUM(variant_observation_count) AS observation_count,
        COUNT(*) AS distinct_payload_count

    FROM payload_variants

    GROUP BY event_id

)

SELECT
    event_id,
    observation_count,
    distinct_payload_count,

    CASE
        WHEN distinct_payload_count = 1
            THEN 'IDENTITY_VERIFIED'
        ELSE 'IDENTITY_CONFLICT'
    END AS identity_status

FROM identity_summary
