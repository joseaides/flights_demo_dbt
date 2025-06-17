{{
	config(
		pre_hook="insert into dwh.log_table (id, start_at, model_name) values('{{ invocation_id}}',clock_timestamp(),'{{this.name}}')",
		post_hook="update  dwh.log_table set end_at=clock_timestamp(), run_time = round(EXTRACT(EPOCH FROM (clock_timestamp() - start_at)) ,2) 
                     where id = '{{ invocation_id}}' and model_name = '{{this.name}}' "
	)
}}

select
    t.*,
    contact_data ->> 'email' as email,
    contact_data ->> 'phone' as phone,
    b.book_date,
    b.total_amount,
    '{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S") }}' as etl_time_str
    from {{ source('stg', 'tickets')}} t 
    left join {{ source('stg', 'bookings')}} b on b.book_ref = t.book_ref

{% if is_incremental() %}
  where b.book_date > coalesce(
                                (select max(book_date) from {{ this }}), 
                                '{{ var("init_date") }}'
                                )
{% endif %}

