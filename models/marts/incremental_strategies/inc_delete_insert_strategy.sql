{{
    config(
        materialized='incremental',
        file_format='delta',
        incremental_strategy='delete+insert',
        unique_key='order_id'
    )
}}

/*
================================================================================
STRATEGY: delete+insert
================================================================================

HOW IT WORKS
------------
dbt runs two statements inside a transaction:
  1. DELETE FROM <target> WHERE order_id IN (SELECT order_id FROM <staging>)
  2. INSERT INTO <target> SELECT * FROM <staging>

Where <staging> is a temporary table/view holding the rows returned by this query.
The unique_key identifies which existing rows to delete before re-inserting.

Contrast with merge:
  - merge   → single MERGE INTO (matched = UPDATE, unmatched = INSERT)
  - delete+insert → DELETE matching keys, then INSERT ALL incoming rows
  The end result for the matched rows is the same, but the mechanism differs.

ADVANTAGES
----------
- Supported on more adapters than merge (some adapters lack MERGE syntax).
- Simpler execution plan than MERGE — two straightforward DML statements.
- Reliable on Spark/Databricks for non-Delta tables where MERGE is unavailable.
- Easier to reason about: "delete old versions, insert new versions."

DISADVANTAGES
-------------
- Not atomic across the two steps on non-transactional engines (risk of partial
  state if the job fails between DELETE and INSERT).
- On Delta Lake, prefer merge or replace_where — they are atomic single operations.
- Slightly more overhead than append; requires a unique_key.
- Cannot selectively update only changed columns (unlike merge_update_columns).

USE CASES
---------
- Adapters or table formats that support DELETE + INSERT but not MERGE.
- Situations where MERGE performance is poor and a bulk delete+reinsert is faster.
- When you want to replace a known set of keys without the complexity of MERGE syntax.
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

-- Filter to a recent lookback window.
-- dbt will DELETE existing rows whose order_id matches, then INSERT these results.
{% if is_incremental() %}
    where order_date >= date_add(
        (select max(order_date) from {{ this }}),
        -3
    )
{% endif %}
