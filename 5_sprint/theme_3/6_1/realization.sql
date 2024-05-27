--STG
drop table if exists stg.bonussystem_users;
CREATE TABLE stg.bonussystem_users (
	id integer not null,
	order_user_id text not null
);

drop table if exists stg.bonussystem_ranks;
CREATE TABLE stg.bonussystem_ranks (
	id integer not null,
	"name" varchar(2048) not null,
	bonus_percent numeric(19, 5) not null,
	min_payment_threshold numeric(19, 5) not null
);

drop index if exists idx_bonussystem_events__event_ts;
drop table if exists stg.bonussystem_events;
CREATE TABLE stg.bonussystem_events (
	id integer not null,
	event_ts timestamp not null,
	event_type varchar not null,
	event_value text not null
);
CREATE INDEX idx_bonussystem_events__event_ts ON stg.bonussystem_events USING btree (event_ts);
