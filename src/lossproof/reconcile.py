"""The measurement: compare every source row with the lake, key by key.

Run it when the source is quiet and the consumer has drained. Each row is reduced to a canonical
string on both sides. The source's current columns define what is compared, so a column the lake
never learned about counts as a difference.
"""

from __future__ import annotations

from collections import Counter
from typing import Any

from lossproof import lake

NULL = "∅"


def canonical(row: dict[str, Any], columns: list[str]) -> str:
    return "|".join(NULL if row.get(c) is None else str(row[c]) for c in columns)


def diff(source: dict[str, str], lake_rows: list[tuple[str, str]]) -> dict[str, int]:
    """Count the ways the lake differs from the source. ``lake_rows`` may repeat a key."""
    copies = Counter(pk for pk, _ in lake_rows)
    lake_by_pk = dict(lake_rows)
    return {
        "source_rows": len(source),
        "lake_rows": len(lake_rows),
        "missing": sum(pk not in lake_by_pk for pk in source),  # in the source, not in the lake
        "ghost": sum(pk not in source for pk in lake_by_pk),  # deleted at the source, still in the lake
        "stale": sum(pk in lake_by_pk and lake_by_pk[pk] != v for pk, v in source.items()),
        "duplicate": sum(n - 1 for n in copies.values()),
    }


def wrong(d: dict[str, int]) -> int:
    return d["missing"] + d["ghost"] + d["stale"] + d["duplicate"]


def source_rows(conn: Any, table: str) -> tuple[list[str], dict[str, str]]:
    """The table's current columns and its rows as {pk: canonical string}."""
    cols = [
        (name, dtype)
        for name, dtype in conn.execute(
            "select column_name, data_type from information_schema.columns "
            "where table_schema = 'public' and table_name = %s order by ordinal_position",
            (table,),
        )
    ]

    def text(name: str, dtype: str) -> str:
        if dtype == "timestamp with time zone":  # the format Debezium emits
            return f"to_char({name} at time zone 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"')"
        return f"{name}::text"

    pk = " || '|' || ".join(f"{c}::text" for c in lake.TABLES[table]["pk"])
    value = " || '|' || ".join(f"coalesce({text(n, t)}, '{NULL}')" for n, t in cols)
    return [n for n, _ in cols], dict(conn.execute(f"select {pk}, {value} from {table}"))


def run(suffix: str) -> dict[str, Any]:
    """Reconcile all tables of one consumer against the source."""
    import psycopg

    from lossproof import config

    cat = lake.catalog()
    tables: dict[str, dict[str, int]] = {}
    with psycopg.connect(config.source_dsn()) as conn:
        lsn = conn.execute("select pg_current_wal_lsn()::text").fetchone()[0]  # type: ignore[index]
        for t in lake.TABLES:
            columns, source = source_rows(conn, t)
            lake_rows = [(r["_pk"], canonical(r, columns)) for r in lake.read(cat, t, suffix)]
            tables[t] = diff(source, lake_rows)
    return {
        "consumer": suffix,
        "source_lsn": lsn,
        "tables": tables,
        "source_rows": sum(d["source_rows"] for d in tables.values()),
        "wrong_rows": sum(wrong(d) for d in tables.values()),
    }
