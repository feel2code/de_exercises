drop table if exists dds.h_restaurant;
create table if not exists dds.h_restaurant (
    h_restaurant_pk uuid primary key,
    restaurant_id varchar not null,
    load_dt timestamp not null,
    load_src varchar not null default 'orders-system-kafka'
);
