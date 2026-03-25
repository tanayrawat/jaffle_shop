{{
    config(
        materialized='incremental',
        incremental_strategy='microbatch',
        event_time='order_date',
        begin='2020-01-01',
        batch_size='day',
        lookback=3
    )
}}

/*
================================================================================
STRATEGY: microbatch  (dbt 1.9+)
================================================================================

HOW IT WORKS
------------
Microbatch is a first-class dbt strategy introduced in dbt-core 1.9. Instead of
you writing is_incremental() filter logic, dbt takes over batch management:

1. dbt determines which time batches are outstanding (using `event_time` and
   `batch_size`).
2. For each batch, dbt runs this model with `event_time` automatically filtered
   to [batch_start, batch_end).
3. Each batch result is atomically written to the target table, replacing any
   prior data for that batch window.
4. `lookback` (default 1) causes dbt to always reprocess the last N batches,
   handling late-arriving data automatically.

Config options:
  event_time  — the timestamp/date column that defines your batches.
  begin       — the earliest date to process on a full refresh.
  batch_size  — 'hour' | 'day' | 'month' | 'year'
  lookback    — number of prior batches to always reprocess (handles late data).

NOTE: Do NOT add an is_incremental() filter — dbt injects the batch filter
      automatically. Writing your own filter will conflict with dbt's logic.

ADVANTAGES
----------
- dbt manages all batch logic — no hand-written is_incremental() filters.
- Automatic retry of individual failed batches without full reprocessing.
- Built-in late-data handling via `lookback`.
- Parallelisable: dbt can process multiple batches concurrently.
- Idempotent: re-running a batch always produces the same result.

DISADVANTAGES
-------------
- Requires dbt-core >= 1.9 and a compatible adapter.
- Only works with time-based partitioning (event_time is mandatory).
- Less flexible than hand-crafted incremental logic for complex scenarios.
- Still relatively new; some edge cases may require workarounds.

USE CASES
---------
- High-volume event tables (billions of rows) where per-batch retries matter.
- Pipelines that must guarantee exactly-once processing per time window.
- Teams that want dbt to own batch orchestration rather than an external scheduler.
- Replacing fragile custom is_incremental() logic with a managed solution.
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
-- No is_incremental() filter here — dbt automatically injects the batch window
-- filter on `order_date` based on batch_size and the current batch being processed.
