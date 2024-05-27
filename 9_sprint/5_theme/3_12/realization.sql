drop table if exists dds.l_product_category;
create table if not exists dds.l_product_category (
    hk_product_category_pk uuid primary key,
    h_product_pk uuid not null references dds.h_product (h_product_pk),
    h_category_pk uuid not null references dds.h_category (h_category_pk),
    load_dt timestamp not null,
    load_src varchar not null default 'orders-system-kafka'
);
