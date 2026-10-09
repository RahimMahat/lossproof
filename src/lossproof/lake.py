"""The lake: one Iceberg table per source table, catalogued in Glue, stored on S3.

Every lake table has the source columns plus ``_pk`` (the primary key as one string, so a batch of
keys is a single IN filter even for composite keys) and ``_lsn`` (the source log position of the
change that produced the row).
"""

from __future__ import annotations

import contextlib
from typing import Any

import pyarrow as pa

from lossproof import config

DATABASE = "lossproof"
BUCKET = "lossproof-lake"

# Source tables as first created. ``updated_at`` is kept as the ISO-8601 UTC string Debezium emits.
TABLES: dict[str, dict[str, Any]] = {
    "customers": {
        "pk": ["id"],
        "cols": {"id": pa.int64(), "email": pa.string(), "tier": pa.string(), "updated_at": pa.string()},
    },
    "orders": {
        "pk": ["id"],
        "cols": {
            "id": pa.int64(),
            "customer_id": pa.int64(),
            "status": pa.string(),
            "total_cents": pa.int64(),
            "updated_at": pa.string(),
        },
    },
    "order_items": {
        "pk": ["order_id", "line_no"],
        "cols": {
            "order_id": pa.int64(),
            "line_no": pa.int32(),
            "sku": pa.string(),
            "qty": pa.int32(),
            "price_cents": pa.int64(),
        },
    },
    "payments": {
        "pk": ["id"],
        "cols": {"id": pa.int64(), "order_id": pa.int64(), "amount_cents": pa.int64(), "status": pa.string()},
    },
}


def pk_of(table: str, row: dict[str, Any]) -> str:
    return "|".join(str(row[c]) for c in TABLES[table]["pk"])


def arrow_schema(table: str) -> pa.Schema:
    return pa.schema([("_pk", pa.string()), ("_lsn", pa.int64()), *TABLES[table]["cols"].items()])


def catalog() -> Any:
    from pyiceberg.catalog import load_catalog

    creds = {"access-key-id": "test", "secret-access-key": "test", "region": "us-east-1"}
    return load_catalog(
        "floci",
        type="glue",
        warehouse=f"s3://{BUCKET}/warehouse",
        **{"glue.endpoint": config.AWS_ENDPOINT, "s3.endpoint": config.AWS_ENDPOINT},
        **{"s3.path-style-access": "true"},
        **{f"{svc}.{k}": v for svc in ("glue", "s3") for k, v in creds.items()},
    )


def table_name(table: str, suffix: str) -> str:
    """Each consumer writes its own set of tables, e.g. ``lossproof.orders_naive``."""
    return f"{DATABASE}.{table}_{suffix}"


def reset(cat: Any, suffix: str) -> None:
    """Drop and recreate one consumer's tables, empty."""
    from pyiceberg.exceptions import NoSuchTableError

    for t in TABLES:
        with contextlib.suppress(NoSuchTableError):
            cat.drop_table(table_name(t, suffix))
        cat.create_table(table_name(t, suffix), schema=arrow_schema(t))


def read(cat: Any, table: str, suffix: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = cat.load_table(table_name(table, suffix)).scan().to_arrow().to_pylist()
    return rows
