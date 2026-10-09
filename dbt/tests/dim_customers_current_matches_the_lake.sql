-- The history path and the merge path must agree: the current versions in the dimension are
-- exactly the customers in the lake, with the same values. Returns the rows found on one side only.
with dim as (
    select customer_id, email, tier from {{ ref("dim_customers") }} where is_current
),

lake as (
    select customer_id, email, tier from {{ ref("stg_customers") }}
)

(select * from dim except select * from lake)
union all
(select * from lake except select * from dim)
