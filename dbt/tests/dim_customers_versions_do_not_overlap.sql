-- A customer has at most one current version, and no version ends before it starts.
select customer_id
from {{ ref("dim_customers") }}
group by customer_id
having count(*) filter (where is_current) > 1 or bool_or(valid_to < valid_from)
