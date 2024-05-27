-- create and insert into table shipping_country_rates
drop table if exists public.shipping_country_rates;
create table public.shipping_country_rates (
	id serial,
	shipping_country text,
	shipping_country_base_rate numeric(14,3),
	primary key (id)
);
insert into public.shipping_country_rates
(shipping_country, shipping_country_base_rate)
select distinct shipping_country, shipping_country_base_rate from public.shipping s;

-- create and insert into table shipping_agreement
drop table if exists public.shipping_agreement;
create table public.shipping_agreement (
	agreement_id serial,
	agreement_number text,
	agreement_rate numeric(14,2),
	agreement_commission numeric(14,2),
	primary key (agreement_id)
);
insert into public.shipping_agreement
(agreement_id, agreement_number, agreement_rate, agreement_commission)
select vendor_agreements[1]::int, vendor_agreements[2]::text, vendor_agreements[3]::numeric(14,2), vendor_agreements[4]::numeric(14,2)
from (
	select (regexp_split_to_array(vendor_agreement_description , E'\\:+')) as vendor_agreements
	from public.shipping
	group by vendor_agreement_description
) as s
order by vendor_agreements[1]::int asc;

-- create and insert into table shipping_transfer
drop table if exists public.shipping_transfer;
create table public.shipping_transfer (
	id serial,
	transfer_type text,
	transfer_model text,
	shipping_transfer_rate numeric(14,3),
	primary key (id)
);
insert into public.shipping_transfer
(transfer_type, transfer_model, shipping_transfer_rate)
select shipping_transfer_description[1], shipping_transfer_description[2], shipping_transfer_rate from (
	select (regexp_split_to_array(shipping_transfer_description, E'\\:+')) as shipping_transfer_description, shipping_transfer_rate
	from public.shipping
group by shipping_transfer_description, shipping_transfer_rate) as s;

-- create and insert into table shipping_info
drop table if exists public.shipping_info;
create table public.shipping_info (
	shipping_id bigint,
	shipping_plan_datetime timestamp,
	payment_amount float4,
	vendor_id bigint,
	shipping_transfer_id bigint,
	shipping_agreement_id bigint,
	shipping_country_rate_id bigint,
	FOREIGN KEY (shipping_transfer_id) REFERENCES shipping_transfer(id) ON UPDATE CASCADE,
	FOREIGN KEY (shipping_agreement_id) REFERENCES shipping_agreement(agreement_id) ON UPDATE CASCADE,
	FOREIGN KEY (shipping_country_rate_id) REFERENCES shipping_country_rates(id) ON UPDATE CASCADE
);
insert into public.shipping_info
(shipping_id, shipping_plan_datetime, payment_amount, vendor_id, shipping_transfer_id, shipping_agreement_id, shipping_country_rate_id)
select distinct s.shipping_id, s.shipping_plan_datetime, s.payment, s.vendor_id, st.id, sa.agreement_id, sc.id
from public.shipping s
join shipping_transfer st on s.shipping_transfer_rate=st.shipping_transfer_rate and s.shipping_transfer_description=(st.transfer_type || ':' || st.transfer_model)
join shipping_agreement sa on (regexp_split_to_array(s.vendor_agreement_description, E'\\:+'))[1]::int=sa.agreement_id
join shipping_country_rates sc on s.shipping_country=sc.shipping_country;


-- create and insert into table shipping_status
drop table if exists public.shipping_status;
create table public.shipping_status (
	shipping_id bigint,
	status text,
	state text,
	shipping_start_fact_datetime timestamp,
	shipping_end_fact_datetime timestamp,
	primary key (shipping_id)
);
insert into public.shipping_status
(shipping_id, status, state, shipping_start_fact_datetime, shipping_end_fact_datetime)
select shipping_id, status, state, shipping_start_fact_datetime, shipping_end_fact_datetime
from shipping s
join (
	select shipping_id, state_datetime as shipping_start_fact_datetime
	from shipping
	where state='booked'
	) as start_state using (shipping_id)
join (
	select shipping_id, state_datetime as shipping_end_fact_datetime
	from shipping
	where state='recieved'
	) as end_state using (shipping_id)
where state_datetime=(select max(state_datetime) from shipping s2 where s2.shipping_id=s.shipping_id)
order by shipping_id;

--shipping_datamart
drop view if exists shipping_datamart;
create view shipping_datamart as
select si.shipping_id, vendor_id, transfer_type,
	   age(shipping_end_fact_datetime, shipping_start_fact_datetime) as full_day_at_shipping,
	   (shipping_end_fact_datetime > shipping_plan_datetime)::int as is_delay,
	   (status = 'finished')::int as is_shipping_finish,
	   shipping_end_fact_datetime, shipping_plan_datetime,
	   (case
		   when (shipping_end_fact_datetime > shipping_plan_datetime)
		   then date_part('day', age(shipping_end_fact_datetime, shipping_plan_datetime))
		   else 0
	   end) as delay_day_at_shipping,
	   payment_amount,
	   (payment_amount * (shipping_country_base_rate + agreement_rate + shipping_transfer_rate)) as vat,
	   (payment_amount * agreement_commission) as profit
from shipping_info si
join shipping_status ss on si.shipping_id=ss.shipping_id
join shipping_transfer st on si.shipping_transfer_id=st.id
join shipping_agreement sa on si.shipping_agreement_id=sa.agreement_id
join shipping_country_rates sc on si.shipping_country_rate_id=sc.id
order by si.shipping_id;
