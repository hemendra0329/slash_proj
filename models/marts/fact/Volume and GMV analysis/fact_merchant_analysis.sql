WITH transactions AS (
    SELECT 
        transaction_id,
        subaccount_id,
        ROUND(transaction_amount_usd, 2) AS transaction_amount_usd,  -- Ensures input is 2 decimals
        merchant_desc,
        merchant_category_code,
        CAST(transaction_timestamp AS DATE) AS transaction_date
    FROM {{ ref('stg_card_transactions') }}
),

merchant_summary AS (
    SELECT 
        merchant_desc,
        merchant_category_code,
        COUNT(DISTINCT transaction_id) AS total_transactions,
        COUNT(DISTINCT subaccount_id) AS unique_customers,
        ROUND(SUM(CASE WHEN transaction_amount_usd < 0 THEN ABS(transaction_amount_usd) ELSE 0 END), 2) AS settled_volume,
        ROUND(SUM(CASE WHEN transaction_amount_usd > 0 THEN transaction_amount_usd ELSE 0 END), 2) AS refund_volume,
        ROUND(AVG(ABS(transaction_amount_usd)), 2) AS avg_transaction_size
    FROM transactions
    GROUP BY merchant_desc, merchant_category_code
),

merchant_retention AS (
    -- Count how many unique months a merchant has appeared in
    SELECT 
        merchant_desc,
        COUNT(DISTINCT EXTRACT(YEAR FROM transaction_date) || '-' || EXTRACT(MONTH FROM transaction_date)) AS active_months
    FROM transactions
    GROUP BY merchant_desc
)

SELECT 
    m.merchant_desc,
    m.merchant_category_code,
    m.total_transactions,
    m.unique_customers,
    m.settled_volume,
    m.refund_volume,
    m.avg_transaction_size,
    r.active_months AS merchant_retention_months
FROM merchant_summary m
LEFT JOIN merchant_retention r ON m.merchant_desc = r.merchant_desc


