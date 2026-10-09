"""The correct consumer. Each rule answers one habit of the careless one.

1. Offsets live in the lake. A table's Kafka offsets are a table property written in the same
   Iceberg commit as its data, and the consumer starts from them. A crash can't separate the two.
2. Changes are merged by key and source log position. A change older than the row it would
   replace is ignored.
3. A batch is one commit per table.
4. Deletes are kept as tombstone rows (``_deleted``), so a delete also has a log position.
5. New source columns are added to the lake table. Integers are always 64-bit.
6. A message that is not a change event goes to the dead-letter queue with its coordinates.
7. When a new snapshot ends, rows it did not refresh are tombstoned: they were deleted while
   nothing was listening.

8. If another writer (the maintenance task) commits first, the batch is read again from the
   lake's offsets and applied to the table as it now is.

Anything else that goes wrong stops the consumer. It restarts from the last commit.
"""

from __future__ import annotations

import json
import logging
import signal
import time
from typing import Any

import pyarrow as pa

from lossproof import config, lake
from lossproof.consume.events import Change, parse

log = logging.getLogger(__name__)
SUFFIX = "correct"
BATCH = 2000
OFFSETS = "lossproof.offsets"
DLQ = "lossproof-dlq"


def schema(table: str) -> pa.Schema:
    cols = [
        (name, pa.int64() if pa.types.is_integer(t) else t) for name, t in lake.TABLES[table]["cols"].items()
    ]
    return pa.schema([("_pk", pa.string()), ("_lsn", pa.int64()), ("_deleted", pa.bool_()), *cols])


def fold_by_lsn(changes: list[Change]) -> dict[str, Change]:
    """Keep the change with the highest log position for each key. Arrival order breaks ties."""
    out: dict[str, Change] = {}
    for c in changes:
        if c.pk not in out or c.lsn >= out[c.pk].lsn:
            out[c.pk] = c
    return out


def newer_than_lake(changes: dict[str, Change], lake_lsn: dict[str, int]) -> dict[str, Change]:
    """Drop changes that are older than what the lake already holds for the key."""
    return {pk: c for pk, c in changes.items() if c.lsn >= lake_lsn.get(pk, -1)}


def to_rows(table: str, changes: dict[str, Change]) -> list[dict[str, Any]]:
    pk_cols = lake.TABLES[table]["pk"]
    rows = []
    for c in changes.values():
        data = c.row if c.row is not None else dict(zip(pk_cols, c.pk.split("|"), strict=True))
        if c.row is None:
            data = {k: int(v) for k, v in data.items()}  # every key column in this schema is an integer
        rows.append({**data, "_pk": c.pk, "_lsn": c.lsn, "_deleted": c.row is None})
    return rows


def add_new_columns(tbl: Any, rows: list[dict[str, Any]]) -> None:
    """Add any column the lake table has not seen. Its own commit: adding a column is harmless to repeat."""
    from pyiceberg.types import BooleanType, DoubleType, LongType, StringType

    iceberg_types = {bool: BooleanType(), int: LongType(), float: DoubleType(), str: StringType()}
    known = {f.name for f in tbl.schema().fields}
    new = {k: type(v) for r in rows for k, v in r.items() if k not in known and v is not None}
    if new:
        with tbl.update_schema() as update:
            for name, py_type in new.items():
                update.add_column(name, iceberg_types[py_type])  # an unknown type stops the consumer


def write(
    cat: Any, table: str, changes: list[Change], offsets: dict[str, int], purge_below: int | None
) -> None:
    """Apply one table's batch, its offsets and any snapshot purge in a single commit."""
    from pyiceberg.expressions import And, EqualTo, In, LessThan

    tbl = cat.load_table(lake.table_name(table, SUFFIX))
    folded = fold_by_lsn(changes)
    if folded:
        seen = tbl.scan(row_filter=In("_pk", list(folded)), selected_fields=("_pk", "_lsn")).to_arrow()  # type: ignore[call-arg, arg-type]
        folded = newer_than_lake(
            folded, dict(zip(seen["_pk"].to_pylist(), seen["_lsn"].to_pylist(), strict=True))
        )
    rows = to_rows(table, folded)
    add_new_columns(tbl, rows)

    purged: list[dict[str, Any]] = []
    stale = None
    if purge_below is not None:
        stale = And(LessThan("_lsn", purge_below), EqualTo("_deleted", False))  # type: ignore[call-arg, arg-type]
        purged = [
            {**r, "_lsn": purge_below, "_deleted": True}
            for r in tbl.scan(row_filter=stale).to_arrow().to_pylist()
            if r["_pk"] not in folded
        ]

    merged = {**json.loads(tbl.properties.get(OFFSETS, "{}")), **offsets}
    arrow = tbl.schema().as_arrow()
    with tbl.transaction() as tx:
        if folded:
            tx.delete(In("_pk", list(folded)))  # type: ignore[call-arg, arg-type]
        if purged:
            tx.delete(In("_pk", [r["_pk"] for r in purged]))  # type: ignore[call-arg, arg-type]
        if rows or purged:
            tx.append(pa.Table.from_pylist(rows + purged, schema=arrow))
        tx.set_properties({OFFSETS: json.dumps(merged)})


def quarantine(sqs: Any, queue_url: str, msg: Any, error: Exception) -> None:
    body = {
        "topic": msg.topic(),
        "partition": msg.partition(),
        "offset": msg.offset(),
        "error": repr(error),
        "value": (msg.value() or b"").decode("utf-8", "replace"),
    }
    sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps(body))


def run(until_idle: float | None = None) -> int:
    """Consume until stopped, or until no message has arrived for ``until_idle`` seconds."""
    from confluent_kafka import OFFSET_BEGINNING, Consumer, TopicPartition
    from pyiceberg.exceptions import CommitFailedException, ValidationException

    from lossproof.tasks import put_metrics

    stopping = False

    def stop(*_: object) -> None:  # SIGTERM: finish the current batch, then leave
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, stop)
    cat = lake.catalog()
    sqs = config.aws("sqs")
    queue_url = sqs.get_queue_url(QueueName=DLQ)["QueueUrl"]
    prefix = f"{config.TOPIC_PREFIX}.public."

    def start_from_lake(consumer: Any, partitions: list[Any]) -> None:  # rule 1
        saved = {
            t: json.loads(cat.load_table(lake.table_name(t, SUFFIX)).properties.get(OFFSETS, "{}"))
            for t in lake.TABLES
        }
        for p in partitions:
            p.offset = saved[p.topic.removeprefix(prefix)].get(str(p.partition), OFFSET_BEGINNING)
        consumer.assign(partitions)

    consumer = Consumer(
        {
            "bootstrap.servers": config.KAFKA_BOOTSTRAP,
            "group.id": f"lossproof-{config.RUN}-{SUFFIX}",
            "enable.auto.commit": False,
            "session.timeout.ms": 6000,  # a killed consumer is replaced quickly
            "topic.metadata.refresh.interval.ms": 5000,  # the topics appear after the first change
        }
    )
    consumer.subscribe([prefix + t for t in lake.TABLES], on_assign=start_from_lake)
    applied, last_message = 0, time.monotonic()
    try:
        while not stopping and (until_idle is None or time.monotonic() - last_message < until_idle):
            changes: dict[str, list[Change]] = {}
            offsets: dict[str, dict[str, int]] = {}
            snapshot_end: int | None = None
            snapshot_table = ""
            for msg in consumer.consume(BATCH, timeout=2):
                if msg.error():
                    continue
                table = (msg.topic() or "").removeprefix(prefix)
                offsets.setdefault(table, {})[str(msg.partition())] = (msg.offset() or 0) + 1
                try:
                    change = parse(msg.topic() or "", msg.value())
                except Exception as e:  # rule 6
                    quarantine(sqs, queue_url, msg, e)
                    continue
                if change is not None:
                    changes.setdefault(table, []).append(change)
                    if change.snapshot == "last":
                        snapshot_end, snapshot_table = change.lsn, table
            if not offsets:
                continue
            # Rule 7. A snapshot ends on one topic but covers every table, so every table is purged.
            # Tables whose snapshot rows are still in flight get them back: those rows carry the
            # snapshot's log position, which is not older than the tombstone. The table that carried
            # the end marker commits last, so a crash part-way replays the marker and purges again.
            tables = list(offsets) if snapshot_end is None else [*lake.TABLES]
            try:
                for table in sorted(tables, key=lambda t: t == snapshot_table):
                    write(cat, table, changes.get(table, []), offsets.get(table, {}), snapshot_end)
            except (CommitFailedException, ValidationException):  # rule 8
                log.warning("another writer committed first; starting again from the lake's offsets")
                consumer.unsubscribe()
                consumer.subscribe([prefix + t for t in lake.TABLES], on_assign=start_from_lake)
                continue
            # The lake is the record. This commit only lets Kafka tooling show the lag.
            consumer.commit(
                offsets=[
                    TopicPartition(prefix + t, int(p), o) for t, po in offsets.items() for p, o in po.items()
                ],
                asynchronous=False,
            )
            batch = [c for cs in changes.values() for c in cs]
            if batch:
                freshness = time.time() - max(c.ts_ms for c in batch) / 1000
                put_metrics({"ChangesApplied": len(batch), "FreshnessSeconds": round(freshness, 3)})
            applied += len(batch)
            last_message = time.monotonic()
    finally:
        consumer.close()
    return applied
