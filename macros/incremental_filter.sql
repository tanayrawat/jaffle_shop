{% macro incremental_filter(column, lookback_days=3) %}
    {% if is_incremental() %}
        where {{ column }} > (
            select dateadd(DAY, -{{ lookback_days }}, max({{ column }}))
            from {{ this }}
        )
    {% endif %}
{% endmacro %}