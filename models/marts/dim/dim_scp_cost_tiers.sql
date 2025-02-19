WITH scp_cost_tiers AS (
    SELECT * FROM (VALUES
        (1, 'Low Volume Cost', 0, 50000, 1.12),
        (2, 'Medium Volume Cost', 50000, 100000, 0.76),
        (3, 'High Volume Cost', 100000, 150000, 0.52),
        (4, 'Max Volume Cost', 150000, NULL, 0.34)
    ) 
    AS t(cost_tier_id, cost_tier_status, min_cards_per_month, max_cards_per_month, card_creation_cost)
)

SELECT * FROM scp_cost_tiers