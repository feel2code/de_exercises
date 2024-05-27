drop function if exists split_deduplication_str;
create or replace function
	split_deduplication_str (str text, sym char)
	returns table (result_value text) as
	$$
	select distinct unnest(regexp_split_to_array(str , '\' || sym || '+' ));
	$$
language sql;

select split_deduplication_str('1:2:1:3',':');

-- результат
--split_deduplication_str|
-------------------------+
--2                      |
--3                      |
--1                      |