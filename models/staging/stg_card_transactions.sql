
WITH source AS (
    SELECT 
        TRANSACTION_ID AS transaction_id,
        SUBACCOUNT_ID AS subaccount_id,
        CARD_ID AS card_id,
        ACCOUNT_TYPE AS account_type,
        TRANSACTION_AMOUNT AS transaction_amount,
        ORIGINAL_CURRENCY_CODE AS original_currency,
        -- Normalizing merchant descriptions by triming spaces and converting to lowercase
        LOWER(TRIM(MERCHANT_DESCRIPTION)) AS merchant_desc,
        MERCHANT_CATEGORY_CODE AS merchant_category_code,
        MERCHANT_COUNTRY AS merchant_country,
        -- Standardize TIMESTAMP format
        CAST(TIMESTAMP AS TIMESTAMP) AS transaction_timestamp
    FROM {{ source('slash', 'card_transactions') }}
),

merchant_category_filled AS (
    SELECT 
        s.*,
        -- Filling missing MCC from previous values for the same merchant
        COALESCE(
            s.merchant_category_code, 
            (SELECT DISTINCT merchant_category_code 
             FROM source s2 
             WHERE s2.merchant_desc = s.merchant_desc 
             AND s2.merchant_category_code IS NOT NULL
             LIMIT 1)
        ) AS final_merchant_category_code
    FROM source s
),

currency_converted AS (
    SELECT 
        m.transaction_id,
        m.subaccount_id,
        m.card_id,
        m.account_type,
        -- Convert to USD using conversion multiplier
        ROUND(
            CASE 
                WHEN m.original_currency = 'USD' THEN m.transaction_amount
                ELSE m.transaction_amount * COALESCE(c.Conversion_Multiplier, 1) 
            END, 2
                ) AS transaction_amount_usd,
        -- Set all transactions to USD after conversion
        'USD' AS original_currency,
        m.merchant_desc,
        m.final_merchant_category_code AS merchant_category_code,
        m.merchant_country,
        m.transaction_timestamp
    FROM merchant_category_filled m
    LEFT JOIN {{ ref('currency_conversion') }} c
    ON m.original_currency = c.Currency
)

SELECT * FROM currency_converted