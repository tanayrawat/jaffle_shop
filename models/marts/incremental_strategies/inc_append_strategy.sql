{{
    config(
        materialized='incremental',
        incremental_strategy='append'
    )
}}

/*
================================================================================
STRATEGY: append
================================================================================

HOW IT WORKS
------------
On the very first run dbt creates the table and inserts all rows.
On every subsequent run dbt evaluates the is_incremental() block, filters the
source to only new rows, then runs a plain INSERT INTO ... SELECT.
No existing rows are ever touched.

Generated SQL (incremental run):
    INSERT INTO <target_table>
    SELECT ... FROM ...
    WHERE order_date > (SELECT MAX(order_date) FROM <target_table>)

ADVANTAGES
----------
- Simplest and fastest strategy — one INSERT, no merge logic.
- Minimal compute; ideal for append-only event streams.
- Works on every dbt adapter; no adapter-specific syntax required.

DISADVANTAGES
-------------
- Cannot handle late-arriving or corrected records — duplicates accumulate.
- Cannot update or delete existing rows.
- Requires careful filter logic; a wrong filter = gaps or double-counts.

USE CASES
---------
- Immutable event logs (clicks, page views, sensor readings).
- Audit / CDC log tables where history must never be overwritten.
- Any strictly append-only source with no corrections or late arrivals.
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

-- is_incremental() is FALSE on the first run so ALL rows are loaded.
-- On subsequent runs only rows newer than what is already in the table are appended.
{% if is_incremental() %}
    where order_date > (select max(order_date) from {{ this }})
{% endif %}
