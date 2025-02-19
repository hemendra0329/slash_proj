
WITH transactions AS (
    SELECT 
        e.entity_id,
        t.subaccount_id,
        t.transaction_id,
        t.transaction_amount_usd * (-1) as transaction_amount_usd,
        t.transaction_timestamp
    FROM {{ ref('stg_entity') }} e
    LEFT JOIN  {{ ref('stg_card_transactions') }} t
        ON e.subaccount_id = t.subaccount_id 
), 

annual_spend AS (
    SELECT 
        t.entity_id,
        EXTRACT(YEAR FROM t.transaction_timestamp) AS year,
        ROUND(SUM(CASE WHEN t.transaction_amount_usd < 0 THEN 0 ELSE t.transaction_amount_usd END),2) AS total_annual_spend   
    FROM transactions t
    GROUP BY t.entity_id, year
)

SELECT 
    a.entity_id,
    a.year,
    a.total_annual_spend,
    d.scp_status,
    d.revenue_boost_percent,
    ROUND(a.total_annual_spend * d.revenue_boost_percent, 2) AS scp_revenue_benefit
FROM annual_spend a
LEFT JOIN {{ ref('dim_scp_revenue_tiers') }} d 
    ON a.total_annual_spend BETWEEN d.min_annual_spend AND COALESCE(d.max_annual_spend, 999999999) -- Handles NULL max_spend
ORDER BY a.year DESC, scp_revenue_benefit DESC

