{{
	config(
		pre_hook="insert into dwh.log_table (id, start_at, model_name) values('{{ invocation_id}}',clock_timestamp(),'{{this.name}}')",
		post_hook=["insert into {{ this}} select -1, null,null,null,'na','na','na','na','na',null"
                   ,"update  dwh.log_table set end_at=clock_timestamp(), run_time = round(EXTRACT(EPOCH FROM (clock_timestamp() - start_at)) ,2) 
                     where id = '{{ invocation_id}}' and model_name = '{{this.name}}' "]
	)
}}

select 
    ad.*,
    case 
	    when ad.range > 5600 then 'high'
	    else 'low'	
    end as range_desc,	
    model ->> 'en' as model_en,
    model ->> 'ru' as model_ru,
    s.seat_no,
    s.fare_conditions,
    '{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S") }}' as etl_time_str
from {{ source('stg', 'aircrafts_data')}} as ad
left join {{ source('stg', 'seats')}} s on s.aircraft_code = ad.aircraft_code