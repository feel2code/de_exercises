drop table if exists stage.user_activity_log;
drop table if exists stage.user_order_log;
drop table if exists stage.customer_research;

CREATE TABLE stage.user_activity_log(
   ID serial ,
   date_time          TIMESTAMP ,
   action_id             BIGINT ,
   customer_id             BIGINT ,
   quantity             BIGINT ,
   PRIMARY KEY (ID)
);

CREATE TABLE stage.user_order_log(
   ID serial ,
   date_time TIMESTAMP,
   city_id integer,
   city_name varchar(100),
   customer_id BIGINT,
   first_name varchar(100),
   last_name varchar(100),
   item_id integer,
   item_name varchar(100),
   quantity BIGINT,
   payment_amount numeric(14,2),
   PRIMARY KEY (ID)
);

CREATE TABLE stage.customer_research(
   ID serial ,
   date_id          TIMESTAMP ,
   category_id      integer ,
   geo_id           integer,
   sales_qty        integer ,
   sales_amt        numeric(14,2) ,
   PRIMARY KEY (ID)
);