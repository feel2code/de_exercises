drop table if exists dds.l_product_restaurant;
create table if not exists dds.l_product_restaurant (
    hk_product_restaurant_pk uuid primary key,
    h_product_pk uuid not null references dds.h_product (h_product_pk),
    h_restaurant_pk uuid not null references dds.h_restaurant (h_restaurant_pk),
    load_dt timestamp not null,
    load_src varchar not null default 'orders-system-kafka'
);
