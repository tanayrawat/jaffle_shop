{{
    config(
        materialized='incremental',
        unique_key='payment_id',
        incremental_strategy='merge'
    )
}}

with incremental_filter as (
    -- compute the max timestamp from the existing target table
    {% if is_incremental() %}
        select max(created_at) as max_created from {{ this }}
    {% else %}
        -- on first run, pull everything
        select cast('1900-01-01' as timestamp) as max_created
    {% endif %}
),

source as (
    select p.*
    from {{ source('stripe', 'payment') }} p
    join incremental_filter
        on p.created > incremental_filter.max_created
),

renamed as (
    select
        id              as payment_id,
        orderid         as order_id,
        paymentmethod   as payment_method,
        status,
        amount / 100    as amount,
        created         as created_at
    from source
)

select * from renamed