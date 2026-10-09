-- Every change to a customer, once. The consumer may append a change twice after a restart,
-- so keep one row per key and log position.
select *
from iceberg_scan('{{ var("customers_history") }}')
qualify row_number() over (partition by _pk, _lsn order by _ts_ms) = 1
