{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='order_id',
        merge_update_columns=['status', 'amount', 'updated_at']
    )
}}

/*
================================================================================
STRATEGY: merge  (UPSERT)
================================================================================

HOW IT WORKS
------------
Uses Delta Lake's MERGE INTO statement. dbt matches incoming rows to existing
rows using `unique_key`. For each match it runs an UPDATE; for each new row it
runs an INSERT. You can restrict which columns get updated via
`merge_update_columns` to avoid overwriting columns you want to keep stable.

Generated SQL (incremental run):
    MERGE INTO <target> AS DBT_INTERNAL_DEST
    USING (SELECT ...) AS DBT_INTERNAL_SOURCE
      ON DBT_INTERNAL_DEST.order_id = DBT_INTERNAL_SOURCE.order_id
    WHEN MATCHED THEN UPDATE SET status = ..., amount = ..., updated_at = ...
    WHEN NOT MATCHED THEN INSERT (order_id, customer_id, ...) VALUES (...)

ADVANTAGES
----------
- True upsert: handles both new rows and corrections to existing rows.
- `merge_update_columns` lets you protect certain columns from being overwritten.
- Efficient: only touches rows that actually changed.
- Ideal for Delta Lake — MERGE INTO is a first-class Delta operation.

DISADVANTAGES
-------------
- More expensive than append — requires a join between source and target.
- Requires a reliable `unique_key`; composite keys add complexity.
- Very large targets can make the MERGE slow if the unique_key is not indexed.

USE CASES
---------
- Order / transaction fact tables where status changes over time.
- Dimension tables (SCD Type 1) where you want the latest value.
- Any table fed by a CDC stream where rows can be inserted or updated.
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
        coalesce(p.amount, 0)    as amount,
        current_timestamp()      as updated_at
    from source_orders o
    left join payments p using (order_id)
)

select * from final

-- Filter to a recent lookback window so the MERGE only processes rows that
-- could plausibly have changed, keeping the source scan small.
{% if is_incremental() %}
    where order_date >= date_add(
        (select max(order_date) from {{ this }}),
        -3
    )
{% endif %}
