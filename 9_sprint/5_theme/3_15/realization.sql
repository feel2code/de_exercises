drop table if exists dds.s_product_names;
create table if not exists dds.s_product_names (
    h_product_pk uuid not null references dds.h_product (h_product_pk),
    name varchar not null,
    hk_product_names_hashdiff uuid not null,
    load_dt timestamp not null,
    load_src varchar not null default 'orders-system-kafka'
);
