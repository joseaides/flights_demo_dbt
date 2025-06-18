
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