
WITH latest_card_status AS (
    SELECT 
        card_id,
        subaccount_id,
        card_status,
        ROW_NUMBER() OVER (PARTITION BY card_id ORDER BY event_timestamp DESC) AS rn
    FROM {{ ref('stg_card_events') }}
)
SELECT 
    card_id, 
    subaccount_id, 
    card_status 
FROM latest_card_status
WHERE rn = 1
