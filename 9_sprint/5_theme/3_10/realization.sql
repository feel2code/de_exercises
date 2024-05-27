drop table if exists dds.l_order_product;
create table if not exists dds.l_order_product (
    hk_order_product_pk uuid primary key,
    h_order_pk uuid not null references dds.h_order (h_order_pk),
    h_product_pk uuid not null references dds.h_product (h_product_pk),
    load_dt timestamp not null,
    load_src varchar not null default 'orders-system-kafka'
);
