WITH source AS (
    SELECT 
        ENTITY_ID AS entity_id,
        SUBACCOUNT_ID AS subaccount_id,
        CAST(ACCOUNT_CREATION_DATE AS TIMESTAMP) AS created_at
    FROM {{ source('slash', 'entity') }}
)
SELECT * FROM source