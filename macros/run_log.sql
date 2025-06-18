{% macro run_log(log_type, model_name) %}
    {% if log_type == 'insert' %}
        INSERT INTO dwh.log_table (id, start_at, model_name) VALUES ('{{ invocation_id}}', clock_timestamp(), '{{model_name}}');
    {% else %}
        UPDATE dwh.log_table SET end_at = clock_timestamp(), run_time = extract(epoch from (clock_timestamp() - start_at)) WHERE id = '{{ invocation_id}}' and model_name = '{{model_name}}';
    {% endif %}

{% endmacro %}