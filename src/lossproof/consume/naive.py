"""The careless consumer. It is correct on a quiet day, and it is how many pipelines start.

Its habits, each one ordinary:
1. Offsets are committed by the client on a timer, whether or not the lake write has happened.
2. Changes are applied in arrival order. The source log position is ignored.
3. A batch is two commits: delete the touched keys, then append the new rows.
4. The lake schema is fixed. Columns the source adds later are dropped.
5. A message that fails to parse is logged and skipped.
"""

from __future__ import annotations

import logging
import time
from typing import Any

import pyarrow as pa

from lossproof import config, lake
from lossproof.consume.events import Change, fold_by_arrival, parse

log = logging.getLogger(__name__)
SUFFIX = "naive"
BATCH = 2000


def write(cat: Any, table: str, changes: dict[str, Change]) -> None:
    from pyiceberg.expressions import In

    tbl = cat.load_table(lake.table_name(table, SUFFIX))
    schema = lake.arrow_schema(table)
    rows = [
        {"_pk": c.pk, "_lsn": c.lsn, **{k: c.row.get(k) for k in lake.TABLES[table]["cols"]}}
        for c in changes.values()
        if c.row is not None
    ]
    tbl.delete(In("_pk", list(changes)))  # type: ignore[call-arg, arg-type]  # pyiceberg typing
    if rows:
        tbl.append(pa.Table.from_pylist(rows, schema=schema))


def run(until_idle: float | None = None) -> int:
    """Consume until killed, or until no message has arrived for ``until_idle`` seconds."""
    from confluent_kafka import Consumer

    consumer = Consumer(
        {
            "bootstrap.servers": config.KAFKA_BOOTSTRAP,
            "group.id": f"lossproof-{SUFFIX}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,  # habit 1
        }
    )
    consumer.subscribe([f"{config.TOPIC_PREFIX}.public.{t}" for t in lake.TABLES])
    cat = lake.catalog()
    applied, last_message = 0, time.monotonic()
    try:
        while until_idle is None or time.monotonic() - last_message < until_idle:
            changes = []
            for msg in consumer.consume(BATCH, timeout=2):
                if msg.error():
                    continue
                try:
                    if (change := parse(msg.topic() or "", msg.value())) is not None:
                        changes.append(change)
                except Exception:  # habit 5
                    log.warning(
                        "skipping bad message at %s[%s]@%s", msg.topic(), msg.partition(), msg.offset()
                    )
            if changes:
                for table, by_pk in fold_by_arrival(changes).items():
                    write(cat, table, by_pk)
                applied += len(changes)
                last_message = time.monotonic()
    finally:
        consumer.close()
    return applied
