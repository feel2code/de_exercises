-- d_city
drop table stage.d_city;
CREATE TABLE IF NOT EXISTS stage.d_city (
    id serial4 NOT NULL,
    city_id int4 NULL,
    city_name varchar(50) NULL
);

insert into stage.d_city (city_id, city_name)
select city_id, city_name from stage.user_order_log
group by 1,2;

-- d_calendar
drop table stage.d_calendar;
CREATE TABLE IF NOT EXISTS stage.d_calendar
(
    date_id serial4,
	day_num smallint,
	month_num smallint,
	month_name varchar(10),
	year_num smallint
);

insert into stage.d_calendar (day_num, month_num, month_name, year_num)
select day_num, month_num, month_name, year_num
from (
select date::date,
       extract(day from date) as day_num,
       extract(month from date) as month_num,
       to_char(date, 'Month') as month_name,
       extract('isoyear' from date) as year_num
  from generate_series(date '2020-01-01',
                       date '2022-01-01',
                       interval '1 day')
       as t(date)
       ) as s;