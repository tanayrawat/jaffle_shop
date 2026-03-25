-- analysis/quarterly_revenue_trends.sql
with orders as (
    select * from {{ ref('fct_orders') }}
),

quarterly as (
    select
        date_trunc('quarter', order_date)  as quarter,
        count(order_id)                    as total_orders,
        sum(amount)                        as total_revenue,
        count(distinct customer_id)        as unique_customers
    from orders
    where status = 'completed'
    group by 1
)

select * from quarterly
order by quarter