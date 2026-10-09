-- Customers as they are now. Tombstones (deleted rows) are left out.
select id as customer_id, email, tier
from iceberg_scan('{{ var("customers") }}')
where not _deleted
