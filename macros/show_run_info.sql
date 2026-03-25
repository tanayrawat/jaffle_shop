{% macro show_run_info() %}
    {{ log('Current target: ' ~ target.name, info=true) }}
    {{ log('Current schema: ' ~ target.schema, info=true) }}
    {{ log('Current database: ' ~ target.database, info=true) }}
{% endmacro %}