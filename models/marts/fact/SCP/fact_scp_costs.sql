WITH card_creation AS (
    SELECT 
        e.entity_id,
        EXTRACT(YEAR FROM ce.event_timestamp) AS year,
        EXTRACT(MONTH FROM ce.event_timestamp) AS month,
        COUNT(DISTINCT ce.card_id) AS cards_created
    FROM {{ ref('stg_entity') }} e
    LEFT JOIN {{ ref('stg_card_events') }} ce 
        ON e.subaccount_id = ce.subaccount_id
    WHERE ce.event_type = 'create'  -- Considering only card creation events
    GROUP BY e.entity_id, year, month
),

scp_costs AS (
    -- Assigning SCP cost tiers based on monthly card creation
    SELECT 
        c.entity_id,
        c.year,
        c.month,
        c.cards_created,
        d.card_creation_cost,
        ROUND(c.cards_created * d.card_creation_cost, 2) AS total_scp_enrollment_cost
    FROM card_creation c
    LEFT JOIN {{ ref('dim_scp_cost_tiers') }} d
        ON c.cards_created BETWEEN d.min_cards_per_month AND COALESCE(d.max_cards_per_month, 999999999)
)

SELECT * FROM scp_costs
ORDER BY year DESC, month DESC, total_scp_enrollment_cost DESC