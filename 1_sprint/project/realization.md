# Витрина RFM

## 1.1. Выясните требования к целевой витрине.

**Data mart:** dm_rfm_segments.

**Schema:** analysis.

**Depth:** с начала 2022 года.

**Contents:**
RFM (от англ. Recency, Frequency, Monetary Value) — способ сегментации клиентов, при котором анализируют их лояльность: как часто, на какие суммы и когда в последний раз тот или иной клиент покупал что-то. На основе этого выбирают клиентские категории, на которые стоит направить маркетинговые усилия. 

Каждого клиента оценивают по трём факторам:

Recency (пер. «давность») — сколько времени прошло с момента последнего заказа.

Frequency (пер. «частота») — количество заказов.

Monetary Value (пер. «денежная ценность») — сумма затрат клиента.

RFM витрина должна включать в себя следующие поля (категории).
user_id
recency (число от 1 до 5)
frequency (число от 1 до 5)
monetary_value (число от 1 до 5)

Фактор Recency измеряется по последнему заказу. Распределите клиентов по шкале от одного до пяти, где значение 1 получат те, кто либо вообще не делал заказов, либо делал их очень давно, а 5 — те, кто заказывал относительно недавно.

Фактор Frequency оценивается по количеству заказов. Распределите клиентов по шкале от одного до пяти, где значение 1 получат клиенты с наименьшим количеством заказов, а 5 — с наибольшим.

Фактор Monetary Value оценивается по потраченной сумме. Распределите клиентов по шкале от одного до пяти, где значение 1 получат клиенты с наименьшей суммой, а 5 — с наибольшей.

**Additional information:**
Витрина не требует обновлений.
Успешно выполненным заказом считается заказ со статусом `Closed`.
В каждой категории должно быть 200 пользователей.
Если у пользователей одинаковое количество заказов то не имеет значение какой из пользователей попадет в ту или иную категорию.

-----------

## 1.2. Изучите структуру исходных данных.

Таблицы необходимые для разработки витрины в базе `production`:

orders - информация о заказах
order_statuses - словарь статусов заказов
orderstatuslog - таблица где хранится информация о смене статусов заказа

Успешно выполненный заказ имеет статус `Closed`, таким образом можно определить завершенные заказы по ключу:
```SQL
select * from production.orders o
left join production.orderstatuses o2 on o2.id=o.status
where o2.key = 'Closed';
```


-----------


## 1.3. Проанализируйте качество данных

```SQL
select * from information_schema.table_constraints tc where constraint_schema='production';
select * from information_schema.constraint_column_usage where table_name='orders';
select * from information_schema.columns where table_name='orders';
select * from information_schema.columns where table_name='orderstatuses';
select order_id, count(*) from orders group by order_id HAVING COUNT(*) > 1;
```

*null данные не найдены, дубликатов нет*

-----------


## 1.4. Подготовьте витрину данных


### 1.4.1. Сделайте VIEW для таблиц из базы production.**


```SQL
create or replace view analysis.Users as (select * from production.users);
create or replace view analysis.OrderItems as (select * from production.OrderItems);
create or replace view analysis.OrderStatuses as (select * from production.OrderStatuses);
create or replace view analysis.Products as (select * from production.Products);
create or replace view analysis.Orders as (select * from production.Orders);
```

### 1.4.2. Напишите DDL-запрос для создания витрины.**

```SQL
drop table if exists analysis.dm_rfm_segments;
create table if not exists analysis.dm_rfm_segments (
  user_id serial primary key,
  recency smallserial check (recency between 1 and 5),
  frequency smallserial check (frequency between 1 and 5),
  monetary_value smallserial check (monetary_value between 1 and 5)
);
CREATE TABLE analysis.tmp_rfm_recency (
 user_id INT NOT NULL PRIMARY KEY,
 recency INT NOT NULL CHECK(recency >= 1 AND recency <= 5)
);
CREATE TABLE analysis.tmp_rfm_frequency (
 user_id INT NOT NULL PRIMARY KEY,
 frequency INT NOT NULL CHECK(frequency >= 1 AND frequency <= 5)
);
CREATE TABLE analysis.tmp_rfm_monetary_value (
 user_id INT NOT NULL PRIMARY KEY,
 monetary_value INT NOT NULL CHECK(monetary_value >= 1 AND monetary_value <= 5)
);
```

### 1.4.3. Напишите SQL запрос для заполнения витрины

```SQL
insert into analysis.tmp_rfm_recency
  select
        user_id,
        ntile(5) over (partition by count(*)/5 order by last_order_dt asc) as recency
	from (
	  select u.id as user_id, coalesce(paid_orders.last_order_dt, to_timestamp('1970-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')) as last_order_dt
	  from (
		select o.user_id, max(order_ts) as last_order_dt
		  from analysis.orders o
		  left join analysis.orderstatuses o2 on o2.id = o.status
		  where o2.key = 'Closed'
		  group by o.user_id
	  ) as paid_orders
	  right join analysis.users u on u.id = paid_orders.user_id
	  order by last_order_dt desc
	  ) as recent_orders
  group by user_id, last_order_dt
  order by recency desc;

insert into analysis.tmp_rfm_frequency
select * from (
with orders_count as (
  select 
        user_id,
        orders_count,
        ROW_NUMBER() OVER(ORDER BY orders_count desc) as row_num
  from (
	select u.id as user_id, coalesce(paid_orders.order_count, 0) as orders_count
	from (
	  select o.user_id, count(*) as order_count
		from analysis.orders o
		left join analysis.orderstatuses o2 on o2.id = o.status
		where o2.key = 'Closed'
		group by o.user_id
	) as paid_orders
	right join analysis.users u on u.id = paid_orders.user_id
	order by orders_count desc
	) as orders_counted
)
select user_id,
 (case
	  when row_num between 1 and 200 then 5
	  when row_num between 201 and 400 then 4
	  when row_num between 401 and 600 then 3
	  when row_num between 601 and 800 then 2
	  when row_num between 801 and 1000 then 1
end) as frequency
from orders_count order by frequency desc) as frequency_tmp;

insert into analysis.tmp_rfm_monetary_value
select * from (
with orders_payments as (
  select 
        user_id,
        payments_sum,
        ROW_NUMBER() OVER(ORDER BY payments_sum desc) as row_num
  from (
	select u.id as user_id, coalesce(paid_orders.order_sum, 0) as payments_sum
	from (
	  select o.user_id, sum(o.payment) as order_sum
		from analysis.orders o
		left join analysis.orderstatuses o2 on o2.id = o.status
		where o2.key = 'Closed'
		group by o.user_id
	) as paid_orders
	right join analysis.users u on u.id = paid_orders.user_id
	order by payments_sum desc
	) as orders_by_payments
)
select user_id,
   (case
	  when row_num between 1 and 200 then 5
	  when row_num between 201 and 400 then 4
	  when row_num between 401 and 600 then 3
	  when row_num between 601 and 800 then 2
	  when row_num between 801 and 1000 then 1
end) as monetary_value
from orders_payments order by monetary_value desc) as monetary_tmp;

insert into analysis.dm_rfm_segments
select r.user_id, r.recency, f.frequency, m.monetary_value
from analysis.tmp_rfm_recency r
left join analysis.tmp_rfm_frequency f on r.user_id=f.user_id
left join analysis.tmp_rfm_monetary_value m on r.user_id=m.user_id
order by r.user_id;
```



