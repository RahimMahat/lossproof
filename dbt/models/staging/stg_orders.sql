select id as order_id, customer_id, status, total_cents, updated_at::timestamptz as updated_at
from iceberg_scan('{{ var("orders") }}')
where not _deleted
