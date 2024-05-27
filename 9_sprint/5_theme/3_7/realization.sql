drop table if exists dds.h_category;
create table if not exists dds.h_category (
    h_category_pk uuid primary key,
    category_name varchar not null,
    load_dt timestamp not null,
    load_src varchar not null default 'orders-system-kafka'
);
