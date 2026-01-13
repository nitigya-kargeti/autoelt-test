{% macro generate_audit_column(column_name) %}
    {% if column_name in ['DW_INSERT_DATE', 'DW_UPDATE_DATE','LOAD_DATE'] %}
        CAST(CURRENT_TIMESTAMP AS TIMESTAMP_TZ) AS {{ column_name }}
    {% else %}
        {{ column_name }}
    {% endif %}
{% endmacro %}
