{% macro delete_from(table_name) %}
    {% if is_incremental() %}
        delete from {{table_name}};
    {% endif %}
{% endmacro %}