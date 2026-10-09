"""Register the Debezium Postgres connector with Kafka Connect (on AWS this would be MSK Connect)."""

from __future__ import annotations

import json
import urllib.request
from typing import Any

from lossproof import config

CONNECTOR = "lossproof-source"


def connector_config() -> dict[str, str]:
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
        "slot.name": "lossproof",
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


def register() -> Any:
    """Create or update the connector. PUT is idempotent."""
    return _call("PUT", f"/connectors/{CONNECTOR}/config", connector_config())


def status() -> Any:
    return _call("GET", f"/connectors/{CONNECTOR}/status")
