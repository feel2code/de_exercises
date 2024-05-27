drop table if exists dds.dm_timestamps;
create table dds.dm_timestamps (
	id serial primary key,
	ts timestamp not null,
	"year" smallint check ("year" >=2022 and "year" < 2500),
	"month" smallint check ("month" >=1 and "month" <= 12),
	"day" smallint check ("day" >=1 and "day" <= 31),
	"time" time not null,
	"date" date not null
);
