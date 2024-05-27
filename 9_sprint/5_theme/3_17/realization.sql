drop table if exists dds.s_order_cost;
create table if not exists dds.s_order_cost (
    h_order_pk uuid not null references dds.h_order (h_order_pk),
    cost decimal(19, 5) not null,
    payment decimal(19, 5) not null,
    hk_order_cost_hashdiff uuid not null,
    load_dt timestamp not null,
    load_src varchar not null default 'orders-system-kafka'
);
