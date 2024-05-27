drop table if exists cdm.user_category_counters;
create table if not exists cdm.user_category_counters (
    id serial primary key,
    user_id uuid not null,
    category_id uuid not null,
    category_name varchar not null,
    order_cnt int not null CHECK (order_cnt > 0),
    unique (user_id, category_id)
);
