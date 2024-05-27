-- d_calendar
delete from mart.d_calendar where 1=1;

CREATE SEQUENCE example_table_seq
    INCREMENT BY 1
    START WITH 1;

insert into mart.d_calendar
(date_id, fact_date, day_num, month_num, month_name, year_num)
with all_dates as (
     select distinct to_date(date_time::TEXT,'YYYY-MM-DD') as date_time from stage.user_activity_log
     union
     select distinct to_date(date_time::TEXT,'YYYY-MM-DD') from stage.user_order_log
     union
     select distinct to_date(date_id::TEXT,'YYYY-MM-DD') from stage.customer_research
     order by date_time
     )
 select nextval('example_table_seq') as date_id,
 date_time as fact_date,
 extract(day from date_time) as day_num,
 extract(month from date_time) as month_num,
 to_char(date_time, 'Month') as month_name,
 extract('isoyear' from date_time) as year_num
 from all_dates;

drop sequence example_table_seq;

-- d_customer
delete from mart.d_customer  where 1=1;
insert into mart.d_customer (customer_id, first_name, last_name, city_id)
select distinct on (customer_id) customer_id, first_name, last_name, city_id
from stage.user_order_log uol;

-- d_item
delete from mart.d_item where 1=1;
insert into mart.d_item (item_id, item_name)
select distinct item_id, item_name from stage.user_order_log;