{{ 
        config(
    materialized= "table"
    ) 
}}

WITH transactions AS (
    SELECT 
        t.transaction_id,
        t.subaccount_id,
        t.card_id,
        t.transaction_amount_usd * (-1) AS transaction_amount_usd,
        t.merchant_desc,
        t.transaction_timestamp,
        CASE 
            WHEN t.merchant_desc SIMILAR TO '%(ticket)%' 
                 OR t.merchant_category_code IN ('7922', '7941') 
            THEN 'Ticket Broker'
            WHEN t.merchant_desc SIMILAR TO '%(facebk|google ads)%' 
                 OR t.merchant_category_code IN ('7311', '5994', '5192') 
            THEN 'Media Buyer'
            ELSE 'Other'
        END AS customer_type
    FROM {{ ref('stg_card_transactions') }} t
)


SELECT * FROM transactions