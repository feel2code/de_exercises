drop table if exists dds.s_restaurant_names;
create table if not exists dds.s_restaurant_names (
    h_restaurant_pk uuid not null references dds.h_restaurant (h_restaurant_pk),
    name varchar not null,
    hk_restaurant_names_hashdiff uuid not null,
    load_dt timestamp not null,
    load_src varchar not null default 'orders-system-kafka'
);
