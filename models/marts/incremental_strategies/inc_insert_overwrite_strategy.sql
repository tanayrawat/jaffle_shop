{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by=['order_date']
    )
}}

/*
================================================================================
STRATEGY: insert_overwrite  (partition replacement)
================================================================================

HOW IT WORKS
------------
Instead of merging row-by-row, dbt identifies which partitions the new data
touches and replaces those entire partitions atomically.

On Databricks the generated SQL looks like:
    INSERT OVERWRITE <target_table>
    PARTITION (order_date)
    SELECT ... FROM ... WHERE order_date IN (<list of new dates>)

Because whole partitions are replaced, you never get duplicates within a
partition — the old data for that date is gone and the new data takes its place.

`partition_by` tells dbt which column(s) define partitions.
The `is_incremental()` filter should return only the partitions you want
to replace; dbt will overwrite exactly those partitions.

ADVANTAGES
----------
- Very fast for wide fact tables: no row-level join needed.
- Atomic partition replacement = no partial-update state.
- Natural fit for date-partitioned tables (daily/hourly batches).
- Great for late-arriving data within a known date range.

DISADVANTAGES
-------------
- Entire partitions are replaced, so ALL rows in that partition must be
  present in your query result — a partial result silently deletes rows.
- Not suitable for tables without a clean partition column.
- Partition granularity must match your arrival pattern (daily data → daily
  partitions; mixed granularity causes over-broad overwrites).

USE CASES
---------
- Daily snapshot / aggregation tables (revenue by day, sessions by day).
- Tables where source data for a given date can be fully recomputed cheaply.
- Late-arriving data that corrects a specific prior day's numbers.
================================================================================
*/

with source_orders as (
    select
        order_id,
        customer_id,
        order_date,
        status
    from {{ ref('stg_jaffle_shop__orders') }}
),

payments as (
    select
        order_id,
        sum(case when status = 'success' then amount end) as amount
    from {{ ref('stg_stripe__payments') }}
    group by 1
),

final as (
    select
        o.order_id,
        o.customer_id,
        o.order_date,
        o.status,
        coalesce(p.amount, 0) as amount
    from source_orders o
    left join payments p using (order_id)
)

select * from final

-- Return ONLY the partitions (dates) you want to overwrite.
-- dbt will replace those partitions entirely, so include ALL rows for each date.
{% if is_incremental() %}
    where order_date >= date_add(current_date(), -3)
{% endif %}
