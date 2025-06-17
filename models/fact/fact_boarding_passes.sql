{{
	config(
		pre_hook="insert into dwh.log_table (id, start_at, model_name) values('{{ invocation_id}}',clock_timestamp(),'{{this.name}}')",
		post_hook="update  dwh.log_table set end_at=clock_timestamp(), run_time = round(EXTRACT(EPOCH FROM (clock_timestamp() - start_at)) ,2) 
                     where id = '{{ invocation_id}}' and model_name = '{{this.name}}' "
	)
}}


select
    tf.*,
    bp.boarding_no ,
    bp.seat_no,
    '{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S") }}' as etl_time_str
from {{ source('stg', 'ticket_flights')}} tf
left join {{ source('stg', 'boarding_passes')}} bp on bp.ticket_no  = tf.ticket_no and bp.flight_id = tf.flight_id


{% if is_incremental() %}
  where tf.last_update > coalesce(
                                (select max(last_update) from {{ this }}), 
                                '{{ var("init_date") }}'
                                )
{% endif %}