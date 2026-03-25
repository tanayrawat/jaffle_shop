{% snapshot scd_jaffle_shop__customers %}

{{
    config(
        target_schema='snapshots',
        unique_key='customer_id',
        strategy='check',
        check_cols=['first_name', 'last_name']
    )
}}

select
    id          as customer_id,
    first_name,
    last_name
from {{ source('jaffle_shop', 'customers') }}

{% endsnapshot %}