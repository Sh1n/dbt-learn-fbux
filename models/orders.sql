with

stg_payments as (
    select * from {{ ref('stg_payments') }}
),

stg_orders as (
    select * from {{ ref('stg_orders') }}
),

final as (

    select
        stg_orders.order_id     as order_id,
        stg_orders.customer_id  as customer_id,
        stg_orders.order_date   as order_date,
        stg_orders.status       as status,
        stg_payments.amount     as amount
    from stg_orders

    left join stg_payments using (order_id)

)

select * from final