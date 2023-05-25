with orders as (

    select * from {{ ref('stg_orders') }}

)
,

payments as (

    select * from {{ ref('stg_payments') }}

)
,

customer_payments as  
(
    select
        orderid as order_id,
        SUM(amount) as spend

    from payments
    where status = 'success'

    group by 1
)
,

final as (

    select
        orders.order_id,
        orders.customer_id,
        orders.status,
        coalesce(customer_payments.spend, 0) as number_of_orders

    from orders

    left join customer_payments using (order_id)

)

select * from final