drop table if exists cdm.user_product_counters;
create table if not exists cdm.user_product_counters (
    id serial primary key,
    user_id uuid not null,
    product_id uuid not null,
    product_name varchar not null,
    order_cnt int not null CHECK (order_cnt > 0),
    unique (user_id, product_id)
);
