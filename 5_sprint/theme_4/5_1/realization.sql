create table public.outbox (
	id integer primary key,
	object_id integer not null,
	record_ts timestamp not null,
	"type" varchar not null,
	payload text not null
);

