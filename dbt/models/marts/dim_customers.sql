-- Slowly changing dimension, type 2: one row per version of a customer.
-- A version is valid from its change until the next change to the same customer.
-- A delete closes the last version and produces no row of its own.
{{ config(location="s3://lossproof-lake/marts/dim_customers/data.parquet") }}

with versions as (
    select
        *,
        to_timestamp(_ts_ms / 1000.0) as valid_from,
        lead(to_timestamp(_ts_ms / 1000.0)) over (partition by id order by _lsn) as valid_to
    from {{ ref("stg_customer_changes") }}
)

select
    md5(_pk || '@' || _lsn) as customer_sk,
    id as customer_id,
    email,
    tier,
    valid_from,
    valid_to,
    valid_to is null as is_current
from versions
where not _deleted
