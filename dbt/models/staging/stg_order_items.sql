select order_id, line_no, sku, qty, price_cents
from iceberg_scan('{{ var("order_items") }}')
where not _deleted
