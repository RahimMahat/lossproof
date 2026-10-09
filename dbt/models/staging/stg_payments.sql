select id as payment_id, order_id, amount_cents, status
from iceberg_scan('{{ var("payments") }}')
where not _deleted
