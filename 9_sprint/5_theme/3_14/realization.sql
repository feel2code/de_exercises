drop table if exists dds.s_user_names;
create table if not exists dds.s_user_names (
    h_user_pk uuid not null references dds.h_user (h_user_pk),
    username varchar not null,
    userlogin varchar not null,
    hk_user_names_hashdiff uuid not null,
    load_dt timestamp not null,
    load_src varchar not null default 'orders-system-kafka'
);
