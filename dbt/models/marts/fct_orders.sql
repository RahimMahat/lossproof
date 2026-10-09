-- One row per live order, with its lines and payment summarised and the customer's current version.
{{ config(location="s3://lossproof-lake/marts/fct_orders/data.parquet") }}

with items as (
    select order_id, count(*) as line_count, sum(qty) as units
    from {{ ref("stg_order_items") }}
    group by order_id
)

select
    o.order_id,
    o.customer_id,
    c.customer_sk,
    o.status,
    o.total_cents,
    coalesce(i.line_count, 0) as line_count,
    coalesce(i.units, 0) as units,
    p.amount_cents as paid_cents,
    p.status as payment_status,
    o.updated_at
from {{ ref("stg_orders") }} as o
left join items as i on i.order_id = o.order_id
left join {{ ref("stg_payments") }} as p on p.order_id = o.order_id
left join {{ ref("dim_customers") }} as c on c.customer_id = o.customer_id and c.is_current
