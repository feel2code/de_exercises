create table d_product_dimensions (
	dimension_id bigint not null,
	category_id bigint null,
	vendor_id bigint null,
	name_product text null,
	vendor_description text null,
	product_id bigint null,
	length numeric(14,2) null,
	width numeric(14, 2) null,
	height numeric(14, 2) null,
	constraint d_product_dimensions_pkey primary key (dimension_id)
);
--select * from d_product_dimensions dpd ;
alter table d_products add column dimension_id bigint default 0;
update d_products as d2 set (dimension_id) = (
	select d.dimension_id from d_product_dimensions d
	join d_products as d1 on d1.product_id=d.product_id
	where d2.product_id=d1.product_id
);
alter table d_product_dimensions drop column category_id;
alter table d_product_dimensions drop column vendor_id;
alter table d_product_dimensions drop column name_product;
alter table d_product_dimensions drop column vendor_description;
alter table d_product_dimensions drop column product_id;

ALTER INDEX IF EXISTS bucket_id RENAME TO bucket_id_index;
