

--------------------------------------------------
--shipping_country_rates

--drop table if exists public.shipping_country_rates;

create table public.shipping_country_rates(
	shipping_country_id serial primary key,
	shipping_country text,
	shipping_country_base_rate numeric(14,3)
);

insert into public.shipping_country_rates
(shipping_country, shipping_country_base_rate)
select distinct 
	shipping_country,
	shipping_country_base_rate
from public.shipping;

--shipping_country_rates
--------------------------------------------------

--------------------------------------------------
--shipping_agreement

--drop table if exists public.shipping_agreement;

create table public.shipping_agreement(
	agreementid bigint primary key,
	agreement_number text,
	agreement_rate numeric(14,2),
	agreement_commission numeric(14,2),
	vendor_agreement_description text
);

insert into public.shipping_agreement
(agreementid, agreement_number, agreement_rate, agreement_commission, vendor_agreement_description)
select 
	vad.description_array[1]::bigint,
	vad.description_array[2],
	vad.description_array[3]::numeric(14,2),
	vad.description_array[4]::numeric(14,2),
	vendor_agreement_description
from (select distinct 
		vendor_agreement_description as vendor_agreement_description, 
		regexp_split_to_array(vendor_agreement_description, ':') as description_array 
		from public.shipping) as vad

--shipping_agreement
--------------------------------------------------

--------------------------------------------------
--shipping_transfer

--drop table if exists public.shipping_transfer;

create table public.shipping_transfer(
	transfer_type_id serial primary key,
	transfer_type text,
	transfer_model text,
	shipping_transfer_rate numeric(14,3),
	shipping_transfer_description text
);

insert into public.shipping_transfer
(transfer_type, transfer_model, shipping_transfer_rate, shipping_transfer_description)
select
	std.description_array[1],
	std.description_array[2],
	std.shipping_transfer_rate::numeric(14,3),
	std.shipping_transfer_description
from (select distinct 
		shipping_transfer_description,
		regexp_split_to_array(shipping_transfer_description, ':') as  description_array, 
		shipping_transfer_rate
		from public.shipping) as std;

--shipping_transfer	
--------------------------------------------------
	
--------------------------------------------------
--shipping_info	
	
--drop table if exists public.shipping_info;

create table public.shipping_info(
	shippingid bigint primary key,
	shipping_country_id bigint references public.shipping_country_rates (shipping_country_id),
	shipping_agreement_id bigint references public.shipping_agreement (agreementid),
	shipping_transfer_id bigint references public.shipping_transfer (transfer_type_id),
	shipping_plan_datetime timestamp,
	payment_amount numeric(14,2),
	vendorid bigint
);

insert into public.shipping_info
(shippingid, shipping_country_id, shipping_agreement_id, shipping_transfer_id, shipping_plan_datetime, payment_amount, vendorid)
select distinct
	shipping.shippingid,
	country.shipping_country_id,
	agreement.agreementid,
	transfer.transfer_type_id,
	shipping.shipping_plan_datetime,
	shipping.payment_amount,
	shipping.vendorid
from public.shipping as shipping
inner join public.shipping_country_rates as country
	on shipping.shipping_country = country.shipping_country 
	and shipping.shipping_country_base_rate = country.shipping_country_base_rate
inner join public.shipping_agreement as agreement
	on shipping.vendor_agreement_description = agreement.vendor_agreement_description
inner join public.shipping_transfer as transfer
	on shipping.shipping_transfer_description = transfer.shipping_transfer_description
	and shipping.shipping_transfer_rate = transfer.shipping_transfer_rate;

--shipping_info	
--------------------------------------------------

--------------------------------------------------
--shipping_status	

--drop table if exists public.shipping_status;

create table public.shipping_status(
	shippingid bigint primary key,
	status text,
	state text,
	shipping_start_fact_datetime timestamp,
	shipping_end_fact_datetime timestamp
);

insert into public.shipping_status
(shippingid, status, state, shipping_start_fact_datetime, shipping_end_fact_datetime)
select distinct
	shippingid,
	last_value(status) over(
		partition by shippingid 
		order by state_datetime 
		range between unbounded preceding and unbounded following) as last_status,
	last_value(state) over(
		partition by shippingid 
		order by state_datetime 
		range between unbounded preceding and unbounded following) as last_state,
	first_value(case when state = 'booked' then state_datetime end) over(
		partition by shippingid 
		order by case when state = 'booked' then state_datetime end 
		range between unbounded preceding and unbounded following) as shipping_start_fact_datetime,
	first_value(case when state = 'recieved' then state_datetime end) over(
		partition by shippingid 
		order by case when state = 'recieved' then state_datetime end 
		range between unbounded preceding and unbounded following) as shipping_end_fact_datetime	
from public.shipping; 

--shipping_status	
--------------------------------------------------


--------------------------------------------------
--Удаление вспомогательных колонок

alter table public.shipping_agreement drop column vendor_agreement_description;
alter table public.shipping_transfer drop column shipping_transfer_description;

--Удаление вспомогательных колонок
--------------------------------------------------


--------------------------------------------------
--shipping_datamart view

create or replace view public.shipping_datamart as
	select 
		info.shippingid as shippingid,
		info.vendorid as vendorid,
		transfer.transfer_type as transfer_type,
		date_part('day', status.shipping_end_fact_datetime - status.shipping_start_fact_datetime) as full_day_at_shipping,
		coalesce((info.shipping_plan_datetime < status.shipping_end_fact_datetime), false)::int4 as is_delay,
		(status.status = 'finished')::int4 as is_shipping_finish,
		case 
			when info.shipping_plan_datetime < status.shipping_end_fact_datetime then
				date_part('day', status.shipping_end_fact_datetime - info.shipping_plan_datetime)
			else 0
		end as delay_day_at_shipping,
		info.payment_amount as payment_amount,
		(info.payment_amount * (country.shipping_country_base_rate + agreement.agreement_rate + transfer.shipping_transfer_rate))::numeric(14,2) as vat,
		(info.payment_amount * agreement.agreement_commission)::numeric(14,2) as profit
	from public.shipping_info as info
	inner join public.shipping_transfer as transfer
	on info.shipping_transfer_id = transfer.transfer_type_id 
	inner join public.shipping_status as status
	on info.shippingid = status.shippingid
	inner join public.shipping_country_rates as country
	on info.shipping_country_id = country.shipping_country_id 
	inner join public.shipping_agreement as agreement
	on info.shipping_agreement_id = agreement.agreementid;
	
	



