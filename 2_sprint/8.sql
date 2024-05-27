-- откат
alter table main_table_new_model add column property_one text null;

update main_table_new_model t
	set
	(property_one) =
	(select
		id_property_one::text || ':' || name_property_one || ':' || date(date_property_one)::text
		from main_table_new_model
		where t.id = id
	)
;

alter table main_table_new_model drop column id_property_one;
alter table main_table_new_model drop column name_property_one;
alter table main_table_new_model drop column date_property_one;
alter table main_table_new_model rename to main_table_old_model;
alter table main_table_old_model rename constraint main_table_new_model_pkey to main_table_old_model_pkey;


