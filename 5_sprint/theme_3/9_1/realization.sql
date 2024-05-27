INSERT INTO cdm.dm_settlement_report
(restaurant_id, restaurant_name, settlement_date, orders_count,
 orders_total_sum, orders_bonus_payment_sum, orders_bonus_granted_sum,
 order_processing_fee, restaurant_reward_sum)
select
	do2.restaurant_id,
	dr.restaurant_name,
	dt.date as settlement_date,
	count(distinct fps.order_id) as orders_count,
	sum(fps.total_sum) as orders_total_sum,
	sum(fps.bonus_payment) as orders_bonus_payment_sum,
	sum(fps.bonus_grant) as orders_bonus_granted_sum,
	(0.25 * sum(fps.total_sum)) as order_processing_fee,
	(0.75 * sum(fps.total_sum) - sum(fps.bonus_payment)) as restaurant_reward_sum
 from dds.fct_product_sales fps
 join dds.dm_orders do2 on fps.order_id=do2.id
 	and do2.order_status = 'CLOSED'
 join dds.dm_timestamps dt on dt.id=do2.timestamp_id
 join dds.dm_products dp on fps.product_id=dp.id
 	and dp.active_to >= current_date
 join dds.dm_restaurants dr on do2.restaurant_id=dr.id
 	and dp.restaurant_id=dr.id
 	and dr.active_to >= current_date
 group by do2.restaurant_id,
 		  dr.restaurant_name,
 		  dt.date
on conflict (restaurant_id, settlement_date)
do update
set
    orders_count = EXCLUDED.orders_count,
    orders_total_sum = EXCLUDED.orders_total_sum,
    orders_bonus_payment_sum = EXCLUDED.orders_bonus_payment_sum,
    orders_bonus_granted_sum = EXCLUDED.orders_bonus_granted_sum,
    order_processing_fee = EXCLUDED.order_processing_fee,
    restaurant_reward_sum = EXCLUDED.restaurant_reward_sum
;
--select * from cdm.dm_settlement_report dsr;
