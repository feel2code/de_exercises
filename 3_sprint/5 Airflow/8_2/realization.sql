--f_activity
delete from mart.f_activity;
alter table mart.f_activity drop column click_number;
INSERT INTO mart.f_activity (activity_id, date_id)
select action_id, date_id
from stage.user_activity_log ual
join mart.d_calendar dc on dc.fact_date=ual.date_time
group by date_id, action_id
order by date_id;
alter table mart.f_activity add click_number serial;

--f_daily_sales
delete from mart.f_daily_sales;

INSERT INTO mart.f_daily_sales  (date_id, item_id, customer_id , price , quantity , payment_amount)
select dc.date_id, uol.item_id, uol.customer_id, avg(payment_amount/quantity) as price,
sum(quantity) as quantity, sum(payment_amount) as payment_amount
from stage.user_order_log uol
join mart.d_calendar dc on dc.fact_date=uol.date_time
group by dc.date_id, uol.item_id, uol.customer_id;