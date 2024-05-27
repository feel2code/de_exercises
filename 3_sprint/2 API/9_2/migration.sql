-- В production-слое должны оказаться следующие поля:
-- Таблица customer_research:
--
--     поле Date_id
--     поле Geo_id
--     поле Sales_qty
--     поле Sales_amt
--
-- Таблица user_activity_log:
--
--     поле Date_time
--     поле Customer_id
--
-- Таблица user_order_log:
--
--     поле Date_time
--     поле Customer_id
--     поле Quantity
--     поле Payment_amount

-- DROP SCHEMA stage;

CREATE SCHEMA prod AUTHORIZATION jovyan;

create table prod.customer_research
as
select scr.date_id, scr.geo_id, scr.sales_qty, scr.sales_amt
from stage.customer_research scr;

create table prod.user_activity_log
as
select ual.date_time, ual.customer_id
from stage.user_activity_log ual;

create table prod.user_order_log
as
select uol.date_time, uol.customer_id, uol.quantity, uol.payment_amount
from stage.user_order_log uol;