WITH scp_revenue AS (
    -- SCP revenue per entity per year
    SELECT 
        entity_id,
        year,
        scp_revenue_benefit
    FROM {{ ref('fact_scp_revenue') }}
),

scp_cost AS (
    -- SCP enrollment cost per entity per year
    SELECT 
        entity_id,
        year,
        SUM(total_scp_enrollment_cost) AS total_scp_enrollment_cost
    FROM {{ ref('fact_scp_costs') }}
    GROUP BY entity_id, year
)

SELECT 
    r.entity_id,
    r.year,
    r.scp_revenue_benefit,
    COALESCE(c.total_scp_enrollment_cost, 0) AS total_scp_enrollment_cost,
    (r.scp_revenue_benefit - COALESCE(c.total_scp_enrollment_cost, 0)) AS net_scp_profit,
    CASE 
        WHEN (r.scp_revenue_benefit - COALESCE(c.total_scp_enrollment_cost, 0)) > 0 THEN 'Profitable'
        ELSE 'Not Profitable'
    END AS scp_enrollment_decision
FROM scp_revenue r
LEFT JOIN scp_cost c 
    ON r.entity_id = c.entity_id AND r.year = c.year
ORDER BY r.year DESC, net_scp_profit DESC