
WITH source AS (
    SELECT 
        SLASH_ACCOUNT_ID AS subaccount_id,
        CARD_EVENT_ID AS event_id,
        CARD_ID AS card_id,
        EVENT_TYPE AS event_type,  
        CASE 
            WHEN CARD_STATUS LIKE 'pending_%' THEN REPLACE(CARD_STATUS, 'pending_', '')
            ELSE CARD_STATUS
        END AS card_status,
        CAST(TIMESTAMP AS TIMESTAMP) AS event_timestamp
    FROM {{ source('slash', 'card_events') }}
)

SELECT * FROM source
