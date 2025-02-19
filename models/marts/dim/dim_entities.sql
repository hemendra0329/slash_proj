WITH source AS (
    SELECT 
        entity_id,
        subaccount_id,
        created_at AS account_creation_date
    FROM {{ ref('stg_entity') }}
),

customer_classification AS (
    SELECT 
        e.entity_id,
        e.subaccount_id,
        e.account_creation_date,
        -- Classifying customers based on merchant transactions
        CASE 
            WHEN t.merchant_desc SIMILAR TO '%(ticket)%' 
                 OR t.merchant_category_code IN ('7922', '7941') 
            THEN 'Ticket Broker'
            WHEN t.merchant_desc SIMILAR TO '%(facebk|google ads)%' 
                 OR t.merchant_category_code IN ('7311', '5994', '5192') 
            THEN 'Media Buyer'
            ELSE 'Other'
        END AS customer_type
    FROM source e
    LEFT JOIN {{ ref('stg_card_transactions') }} t 
    ON e.subaccount_id = t.subaccount_id
)

SELECT 
    entity_id,
    subaccount_id,
    account_creation_date,
    customer_type
FROM customer_classification 