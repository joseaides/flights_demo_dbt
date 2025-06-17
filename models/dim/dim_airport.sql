{{
	config(
		pre_hook="insert into dwh.log_table (id, start_at, model_name) values('{{ invocation_id}}',clock_timestamp(),'{{this.name}}')",
		post_hook=["insert into {{ this}} select -1, null,null,null,'na',null,'na','na','na','na'"
                   ,"update  dwh.log_table set end_at=clock_timestamp(), run_time = round(EXTRACT(EPOCH FROM (clock_timestamp() - start_at)) ,2) 
                     where id = '{{ invocation_id}}' and model_name = '{{this.name}}' "]
	)
}}


select 
    a.*,
    airport_name ->> 'en' as airport_name_en,
    airport_name ->> 'ru' as airport_name_ru,
    city ->> 'en' as city_name_en,
    city ->> 'ru' as city_name_ru
from stg.airports_data a
