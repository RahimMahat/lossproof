"""Turn Debezium change events into per-table batches. Pure functions, shared by both consumers."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from lossproof import lake


@dataclass(frozen=True)
class Change:
    table: str
    pk: str
    lsn: int
    row: dict[str, Any] | None  # None means the row was deleted
    snapshot: str = "false"  # Debezium's marker: "false" when streaming, "last" on a snapshot's final row
    ts_ms: int = 0  # when the change was committed at the source


def parse(topic: str, value: bytes | None) -> Change | None:
    """Parse one message. Returns None for a tombstone (the empty message Debezium sends after a delete)."""
    if value is None:
        return None
    event = json.loads(value)
    table = topic.rsplit(".", 1)[-1]
    deleted = event["op"] == "d"
    return Change(
        table=table,
        pk=lake.pk_of(table, event["before"] if deleted else event["after"]),
        lsn=int(event["source"]["lsn"]),
        row=None if deleted else event["after"],
        snapshot=str(event["source"].get("snapshot") or "false"),
        ts_ms=int(event["source"].get("ts_ms") or 0),
    )


def fold_by_arrival(changes: list[Change]) -> dict[str, dict[str, Change]]:
    """Per table, keep the last change that arrived for each key."""
    out: dict[str, dict[str, Change]] = {}
    for c in changes:
        out.setdefault(c.table, {})[c.pk] = c
    return out
