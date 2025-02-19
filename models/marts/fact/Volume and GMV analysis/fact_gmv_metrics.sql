WITH transactions AS (
    SELECT 
        transaction_id,
        subaccount_id,
        transaction_amount_usd,
        merchant_desc,
        merchant_category_code,
        CAST(transaction_timestamp AS DATE) AS transaction_date,
        EXTRACT(YEAR FROM transaction_timestamp) AS year,
        EXTRACT(MONTH FROM transaction_timestamp) AS month
    FROM {{ ref('stg_card_transactions') }}
),

gmv_summary AS (
    SELECT 
        year,
        month,
        COUNT(DISTINCT transaction_id) AS total_transactions,
        COUNT(DISTINCT subaccount_id) AS active_customers,
        SUM(CASE WHEN transaction_amount_usd < 0 THEN ABS(transaction_amount_usd) ELSE 0 END) AS settled_volume,
        SUM(CASE WHEN transaction_amount_usd > 0 THEN transaction_amount_usd ELSE 0 END) AS refund_volume,
        SUM(CASE WHEN transaction_amount_usd < 0 THEN ABS(transaction_amount_usd) ELSE 0 END) 
        - SUM(CASE WHEN transaction_amount_usd > 0 THEN transaction_amount_usd ELSE 0 END) AS net_gmv
    FROM transactions
    GROUP BY year, month
)

SELECT * FROM gmv_summary