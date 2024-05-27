-- Удалите внешний ключ из sales

-- Удалите первичный ключ из products

-- Добавьте новое поле id для суррогантного ключа в products

-- Сделайте данное поле первичным ключом

-- Добавьте дату начала действия записи в products

-- Добавьте дату окончания действия записи в products

-- Добавьте новый внешний ключ sales_products_id_fk в sales

alter table sales drop constraint sales_products_product_id_fk;
alter table products drop constraint products_pk;

alter table products
add column id serial,
add column valid_from timestamptz not null,
add column valid_to timestamptz null;

alter table products add constraint products_pk primary key (id);
alter table sales add constraint sales_products_product_id_fk foreign key (product_id) references products (id);
