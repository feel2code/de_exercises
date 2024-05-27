drop table if exists dds.l_order_user;
create table if not exists dds.l_order_user (
    hk_order_user_pk uuid primary key,
    h_order_pk uuid not null references dds.h_order (h_order_pk),
    h_user_pk uuid not null references dds.h_user (h_user_pk),
    load_dt timestamp not null,
    load_src varchar not null default 'orders-system-kafka'
);
