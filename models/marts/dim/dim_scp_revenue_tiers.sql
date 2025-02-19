
WITH scp_revenue_tiers AS (
    SELECT * FROM (VALUES
        (1, 'Tier 1', 20000, 40000, 0.0015),
        (2, 'Tier 2', 40000, 100000, 0.0020),
        (3, 'Tier 3', 100000, 250000, 0.0030),
        (4, 'Tier 4', 250000, NULL, 0.0035)
    ) 
    AS t(tier_id, scp_status, min_annual_spend, max_annual_spend, revenue_boost_percent)
)

SELECT * FROM scp_revenue_tiers