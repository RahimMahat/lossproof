"""Settings from the environment. Defaults suit the app container on the Compose network."""

from __future__ import annotations

import json
import os
from typing import Any

AWS_ENDPOINT = os.environ.get("AWS_ENDPOINT_URL", "http://floci:4566")
KAFKA_BOOTSTRAP = os.environ.get("LOSSPROOF_KAFKA", "")
PG_HOST = os.environ.get("LOSSPROOF_PG_HOST", "")
CONNECT_URL = os.environ.get("LOSSPROOF_CONNECT", "http://connect:8083")

SOURCE_SECRET = "lossproof/source-db"
TOPIC_PREFIX = "lp"
SEED = 7


def aws(service: str) -> Any:
    import boto3

    # Floci accepts any credentials.
    return boto3.client(
        service,
        endpoint_url=AWS_ENDPOINT,
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )


def source_credentials() -> dict[str, str]:
    """Database user, password and name, read from Secrets Manager."""
    secret: dict[str, str] = json.loads(
        aws("secretsmanager").get_secret_value(SecretId=SOURCE_SECRET)["SecretString"]
    )
    return secret


def source_dsn() -> str:
    c = source_credentials()
    return f"host={PG_HOST} port=5432 user={c['username']} password={c['password']} dbname={c['dbname']}"
