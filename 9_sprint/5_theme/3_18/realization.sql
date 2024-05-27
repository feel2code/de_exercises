drop table if exists dds.s_order_status;
create table if not exists dds.s_order_status (
    h_order_pk uuid not null references dds.h_order (h_order_pk),
    status varchar not null,
    hk_order_status_hashdiff uuid not null,
    load_dt timestamp not null,
    load_src varchar not null default 'orders-system-kafka'
);
