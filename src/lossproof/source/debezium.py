"""Register the Debezium Postgres connector with Kafka Connect (on AWS this would be MSK Connect)."""

from __future__ import annotations

import json
import time
import urllib.request
from typing import Any

from lossproof import config


def connector_config(slot: str) -> dict[str, str]:
    c = config.source_credentials()
    return {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "plugin.name": "pgoutput",
        "database.hostname": config.PG_HOST,
        "database.port": "5432",
        "database.user": c["username"],
        "database.password": c["password"],
        "database.dbname": c["dbname"],
        "topic.prefix": config.TOPIC_PREFIX,
        "schema.include.list": "public",
        "slot.name": slot,
        "publication.name": slot,
        # Plain JSON values without the embedded schema: {"before", "after", "op", "source": {"lsn", ...}}
        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "false",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false",
        # Three partitions per table. Events are keyed by primary key, so order holds per row
        # but not across rows or tables.
        "topic.creation.default.partitions": "3",
        "topic.creation.default.replication.factor": "1",
    }


def _call(method: str, path: str, body: dict[str, Any] | None = None) -> Any:
    req = urllib.request.Request(
        f"{config.CONNECT_URL}{path}",
        method=method,
        data=json.dumps(body).encode() if body is not None else None,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read() or b"null")


def register(generation: int = 1) -> Any:
    """Create or update the connector. PUT is idempotent.

    A new generation is a new connector with a new replication slot writing to the same topics:
    it starts with a snapshot of every row, as after a lost slot.

    The name carries the time. Kafka Connect remembers a connector's position by name, and a
    connector registered after a reset must not inherit the position of the one before it.
    """
    stamp = int(time.time())
    slot = f"lp_{config.RUN}_{generation}_{stamp}"
    return _call(
        "PUT", f"/connectors/lossproof-{config.RUN}-{generation}-{stamp}/config", connector_config(slot)
    )


def remove_all() -> list[str]:
    """Delete every connector. Their replication slots stay behind until dropped."""
    names: list[str] = _call("GET", "/connectors")
    for name in names:
        _call("DELETE", f"/connectors/{name}")
    return names


def status() -> Any:
    return {name: _call("GET", f"/connectors/{name}/status") for name in _call("GET", "/connectors")}
