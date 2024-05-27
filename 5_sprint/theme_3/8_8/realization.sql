alter table dds.dm_orders add constraint
dm_orders_user_id_fk foreign key (user_id) references dds.dm_users (id);
alter table dds.dm_orders add constraint
dm_orders_restaurant_id_fk foreign key (restaurant_id) references dds.dm_restaurants (id);
alter table dds.dm_orders add constraint
dm_orders_timestamp_id_fk foreign key (timestamp_id) references dds.dm_timestamps (id);
