drop table if exists stg.order_events;
create table if not exists stg.order_events (
    id serial primary key,
    object_id int not null,
    payload json not null,
    object_type varchar not null,
    sent_dttm timestamp not null,
    unique (object_id)
);
