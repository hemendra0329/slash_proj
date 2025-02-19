WITH source AS (
    SELECT 
        LOWER(TRIM(merchant_desc)) AS merchant_desc, -- Standardizing merchant name
        merchant_category_code,
        merchant_country
    FROM {{ ref('stg_card_transactions') }}
),

unique_merchants AS (
    SELECT 
        merchant_desc,
        merchant_category_code,
        merchant_country
    FROM source
    GROUP BY merchant_desc, merchant_category_code, merchant_country
)

SELECT * FROM unique_merchants