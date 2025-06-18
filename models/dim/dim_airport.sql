{{
	config(
		post_hook="insert into {{ this}} select -1, null,null,null,'na',null,'na','na','na','na'"
  )                 
}}


select 
    a.*,
    airport_name ->> 'en' as airport_name_en,
    airport_name ->> 'ru' as airport_name_ru,
    city ->> 'en' as city_name_en,
    city ->> 'ru' as city_name_ru
from stg.airports_data a
