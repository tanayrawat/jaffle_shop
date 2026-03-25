select
    id as order_id,
    user_id as customer_id,
    order_date,
    status

from {{source('jaffle_shop', 'orders')}}
{{ limit_in_dev('order_date', n=30) }}
