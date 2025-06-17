{{
	config(
		pre_hook="insert into dwh.log_table (id, start_at, model_name) values('{{ invocation_id}}',clock_timestamp(),'{{this.name}}')",
		post_hook="update  dwh.log_table set end_at=clock_timestamp(), run_time = round(EXTRACT(EPOCH FROM (clock_timestamp() - start_at)) ,2) 
                     where id = '{{ invocation_id}}' and model_name = '{{this.name}}' "
	)
}}


with flight_duration as 
(
select 
	flight_id ,
	round((extract(epoch from scheduled_arrival - scheduled_departure)/3600)::numeric,2) as flight_duration_expected,
	round((extract(epoch from actual_arrival - actual_departure)/3600)::numeric,2) as flight_duration
from stg.flights 
)
select
	f.*,
	fd.flight_duration_expected,
	fd.flight_duration,
	case when f.actual_departure is null then null
        else (
                case when (fd.flight_duration_expected > fd.flight_duration) then 'Shorter'
	            when (fd.flight_duration_expected < fd.flight_duration) then 'Longer'
	            else 'As Expected'
                end
             )   
	end as duration_actual_vs_expected,
    case when arrival.airport_code is null then '-1' else arrival.airport_code end as arrival_airport_code_chk,
    case when depart.airport_code is null then '-1' else depart.airport_code end as departure_airport_code_chk,
    case when airc.aircraft_code is null then '-1' else airc.aircraft_code end as aircraft_code_chk,
    '{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S") }}' as etl_time_str
from stg.flights f 
left join flight_duration fd on fd.flight_id = f.flight_id
left join {{ ref('dim_airport') }} arrival on arrival.airport_code = f.arrival_airport
left join {{ ref('dim_airport') }} depart on depart.airport_code = f.departure_airport
left join {{ ref('dim_aircraft') }} airc on airc.aircraft_code = f.aircraft_code

{% if is_incremental() %}
  where f.last_update > coalesce(
                                (select max(last_update) from {{ this }}), 
                                '{{ var("init_date") }}'
                                )
{% endif %}
