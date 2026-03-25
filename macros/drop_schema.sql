{% macro drop_schema(schema_name) %}
    {#
        drop_schema(schema_name)
        ========================
        Drops an entire schema/database from Databricks. Used by the CI workflow
        to clean up the temporary PR-specific schema after each PR run.

        Usage:
            dbt run-operation drop_schema --args "{'schema_name': 'dbt_pr_42'}"

        WHY THIS EXISTS
        ---------------
        Each CI run writes to a dedicated schema (dbt_pr_<number>) so that
        different PRs don't clobber each other's tables. Without cleanup, these
        schemas would accumulate indefinitely in Databricks. This macro drops the
        schema and all tables inside it at the end of the CI job.

        SAFETY
        ------
        The IF EXISTS clause makes this idempotent — safe to call even if the
        schema was never created (e.g. the CI run failed before any models ran).
        CASCADE drops all tables inside the schema first.
    #}

    {% set drop_sql %}
        DROP SCHEMA IF EXISTS {{ schema_name }} CASCADE
    {% endset %}

    {% do run_query(drop_sql) %}
    {% do log("Dropped schema: " ~ schema_name, info=True) %}

{% endmacro %}
