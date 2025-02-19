select distinct original_currency from stg_card_transactions;
SELECT * FROM stg_card_transactions;
select * from stg_entity;
select * from stg_card_events; 
select COUNT(*) from card_events; 

select * from dim_cards;

select COUNT(*) from fact_gmv_metrics;

SELECT COUNT(*) FROM fact_customer_segmentation;

select distinct card_status from dim_cards;
select count(*) from stg_card_transactions;

select count(distinct card_id) from fact_card_activity
where customer_type IS NOT NULL
GROUP BY customer_type;

select count(distinct card_id) from fact_card_activity;

SELECT card_id, COUNT(DISTINCT customer_type) AS num_types
FROM fact_customer_segmentation
GROUP BY card_id
HAVING COUNT(DISTINCT customer_type) > 1
ORDER BY num_types DESC;

select * from dim_scp_cost_tiers;

select distinct event_type from stg_card_events;