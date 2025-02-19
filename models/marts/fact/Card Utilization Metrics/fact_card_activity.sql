{{ 
        config(
    materialized= "table"
    ) 
}}

SELECT 
    c.*, 
    CASE WHEN t.transaction_id IS NULL THEN 'Un-utilized' ELSE 'Utilized' END AS Utilization, 
    t.transaction_id,
    t.subaccount_id,
    t.transaction_amount_usd,
    t.merchant_desc,
    -- t.merchant_category_code,
    t.transaction_timestamp,
    t.customer_type
FROM {{ ref('dim_cards') }} c
LEFT JOIN {{ ref('fact_customer_segmentation') }} t
    ON c.card_id = t.card_id