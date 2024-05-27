drop table if exists dds.h_order;
create table if not exists dds.h_order (
    h_order_pk uuid primary key,
    order_id int not null,
    order_dt timestamp not null,
    load_dt timestamp not null,
    load_src varchar not null default 'orders-system-kafka'
);
