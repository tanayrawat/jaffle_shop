{{
    config(
        materialized='incremental',
        incremental_strategy='replace_where',
        incremental_predicates=[
            "order_date >= date_add(current_date(), -3)"
        ]
    )
}}

/*
================================================================================
STRATEGY: replace_where  (Delta Lake — predicate-based replacement)
================================================================================

HOW IT WORKS
------------
`replace_where` is a Delta Lake-native strategy available only on Databricks
(and other Delta-compatible adapters). It uses Delta's `replaceWhere` option to
atomically delete all rows matching the `incremental_predicates` expression from
the target table, then insert the rows returned by this query.

Generated SQL (incremental run):
    -- Internally uses Delta's DataFrameWriter.option("replaceWhere", ...)
    -- Equivalent to:
    DELETE FROM <target> WHERE order_date >= date_add(current_date(), -3);
    INSERT INTO <target> SELECT ... WHERE order_date >= date_add(current_date(), -3);
    -- (done atomically as a single Delta transaction)

The key difference from insert_overwrite:
  - insert_overwrite works at the partition boundary (full partition replacement).
  - replace_where works at any arbitrary predicate — you can replace "last 3 days"
    even if the table is partitioned by month.

ADVANTAGES
----------
- Predicate-based: more flexible than insert_overwrite (no strict partition alignment).
- Atomic: Delta handles the delete+insert as one transaction — no partial state.
- No row-level join: faster than merge for bulk replacements.
- Works well with Delta's optimistic concurrency and time-travel.

DISADVANTAGES
-------------
- Delta Lake / Databricks only — not portable to other warehouses.
- The predicate in `incremental_predicates` must exactly match the WHERE clause
  in this query; a mismatch causes data loss (rows deleted but not re-inserted).
- Not suitable when you need to update individual columns rather than replace rows.

USE CASES
---------
- Rolling-window reprocessing (recompute the last N days of a fact table).
- Tables with late-arriving data that spans a date range, not exact partitions.
- Any Delta table where you want atomic bulk replacement without partition constraints.
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

-- IMPORTANT: this WHERE clause must match the incremental_predicates above.
-- dbt will delete rows matching the predicate, then insert these results.
-- Any rows in the predicate window that are NOT returned here will be lost.
{% if is_incremental() %}
    where order_date >= date_add(current_date(), -3)
{% endif %}
