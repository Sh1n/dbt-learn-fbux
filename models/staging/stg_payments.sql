/*
select
    payment_id,
    order_id,
    amount,
    created_at
from (
    select
      id              as payment_id,
      orderid         as order_id,
      amount / 100    as amount,
      created         as created_at,
      row_number() over (partition by orderid order by _batched_at desc)  as rn
    from raw.stripe.payment
    where status = 'success'
    order by orderid
) as processed_payments
where processed_payments.rn = 1
*/

select
  orderid           as order_id,
  SUM(amount) / 100 as amount
from raw.stripe.payment
where status = 'success'
group by order_id