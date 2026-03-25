{% macro limit_in_dev(column, n=1000) %}
    {% if target.name != 'prod' %}
        where {{ column }} >= dateadd(DAY, -{{ n }}, current_date())
    {% endif %}
{% endmacro %}