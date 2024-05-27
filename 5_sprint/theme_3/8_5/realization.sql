drop table if exists dds.dm_products;
create table dds.dm_products (
	id serial primary key,
	restaurant_id integer not null constraint dm_products_restaurant_id_fkey references dds.dm_restaurants (id),
	product_id varchar not null,
	product_name varchar not null,
	product_price numeric(14, 2) default 0 not null constraint dm_products_product_price_check check (product_price >=0),
	active_from timestamp not null,
	active_to timestamp not null
);
