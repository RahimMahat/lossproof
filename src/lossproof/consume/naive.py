"""The careless consumer. It is correct on a quiet day, and it is how many pipelines start.

Its habits, each one ordinary:
1. Offsets are committed by the client on a timer, whether or not the lake write has happened.
2. Changes are applied in arrival order. The source log position is ignored.
3. A batch is two commits: delete the touched keys, then append the new rows.
4. The lake schema is fixed. Columns the source adds later are dropped.
5. An error is logged and the loop carries on.
"""

from __future__ import annotations

import logging
import signal
import time
from typing import Any

import pyarrow as pa

from lossproof import config, lake
from lossproof.consume.events import Change, fold_by_arrival, parse

log = logging.getLogger(__name__)
SUFFIX = "naive"
schema = lake.arrow_schema
TABLE_NAMES = tuple(lake.TABLES)
BATCH = 2000


def write(cat: Any, table: str, changes: dict[str, Change]) -> None:
    from pyiceberg.expressions import In

    tbl = cat.load_table(lake.table_name(table, SUFFIX))
    schema = lake.arrow_schema(table)
    rows = [
        {"_pk": c.pk, "_lsn": c.lsn, **{k: c.row.get(k) for k in lake.TABLES[table]["cols"]}}  # habit 4
        for c in changes.values()
        if c.row is not None
    ]
    tbl.delete(In("_pk", list(changes)))  # type: ignore[call-arg, arg-type]  # pyiceberg typing
    if rows:
        tbl.append(pa.Table.from_pylist(rows, schema=schema))  # habit 3: a second commit


def run(until_idle: float | None = None) -> int:
    """Consume until stopped, or until no message has arrived for ``until_idle`` seconds."""
    from confluent_kafka import Consumer

    stopping = False

    def stop(*_: object) -> None:  # SIGTERM: finish the current batch, then leave
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, stop)
    consumer = Consumer(
        {
            "bootstrap.servers": config.KAFKA_BOOTSTRAP,
            "group.id": f"lossproof-{config.RUN}-{SUFFIX}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,  # habit 1
            "session.timeout.ms": 6000,  # a killed consumer is replaced quickly
            "topic.metadata.refresh.interval.ms": 5000,  # the topics appear after the first change
        }
    )
    consumer.subscribe([f"{config.TOPIC_PREFIX}.public.{t}" for t in lake.TABLES])
    cat = lake.catalog()
    applied, last_message = 0, time.monotonic()
    try:
        while not stopping and (until_idle is None or time.monotonic() - last_message < until_idle):
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
                for table, by_pk in fold_by_arrival(changes).items():  # habit 2
                    try:
                        write(cat, table, by_pk)
                    except Exception:  # habit 5
                        log.exception("write to %s failed, carrying on", table)
                applied += len(changes)
                last_message = time.monotonic()
    finally:
        consumer.close()
    return applied
