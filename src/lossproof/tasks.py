"""Scheduled maintenance tasks. Each one is a Lambda, run in order by the Step Functions workflow.

They act on the correct consumer's tables. The consumer may be writing at the same time: Iceberg
commits are optimistic, so the loser of a race retries or starts again from the lake's offsets.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime, timedelta
from typing import Any

from lossproof import config, lake
from lossproof import reconcile as rec

log = logging.getLogger(__name__)
CONSUMER = "correct"
MAX_FILES = 8  # compact a table once it has more data files than this
KEEP_SNAPSHOTS_FOR = timedelta(hours=1)
NAMESPACE = "Lossproof"


def maintain() -> dict[str, Any]:
    """Compact tables that have fragmented into many small files, and expire old snapshots."""
    from pyiceberg.exceptions import CommitFailedException, ValidationException

    cat = lake.catalog()
    out: dict[str, Any] = {}
    for t in lake.TABLES:
        tbl = cat.load_table(lake.table_name(t, CONSUMER))
        files = tbl.inspect.files().num_rows
        try:
            if files > MAX_FILES:
                # ponytail: rewrites the whole table in memory; rewrite per partition once tables outgrow RAM.
                tbl.overwrite(tbl.scan().to_arrow())
            snapshots = len(tbl.refresh().snapshots())
            tbl.maintenance.expire_snapshots().older_than(datetime.now(UTC) - KEEP_SNAPSHOTS_FOR).commit()
        except (CommitFailedException, ValidationException):
            # The consumer committed while this was being prepared. Its data wins; maintenance
            # can wait for the next run.
            out[t] = {"files_before": files, "skipped": "the consumer committed first"}
            continue
        tbl.refresh()
        out[t] = {
            "files_before": files,
            "files_after": tbl.inspect.files().num_rows,
            "snapshots_expired": snapshots - len(tbl.snapshots()),
        }
    return out


def reconcile() -> dict[str, Any]:
    """Reconcile the lake with the source, store the report on S3 and publish the count as a metric."""
    result = rec.run(CONSUMER)
    body = json.dumps(result, indent=2).encode()
    s3 = config.aws("s3")
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    for key in (f"reports/reconcile/{stamp}.json", "reports/reconcile/latest.json"):
        s3.put_object(Bucket=lake.BUCKET, Key=key, Body=body, ContentType="application/json")
    put_metrics({"WrongRows": result["wrong_rows"], "SourceRows": result["source_rows"]})
    return {"wrong_rows": result["wrong_rows"], "source_rows": result["source_rows"], "report": stamp}


def put_metrics(values: dict[str, float]) -> None:
    """Publish to CloudWatch. Best effort: a metrics outage must not stop the pipeline."""
    try:
        config.aws("cloudwatch").put_metric_data(
            Namespace=NAMESPACE,
            MetricData=[
                {"MetricName": k, "Value": v, "Dimensions": [{"Name": "Run", "Value": config.RUN}]}
                for k, v in values.items()
            ],
        )
    except Exception:
        log.exception("could not publish metrics")


def marts() -> dict[str, Any]:
    """Build and test the dbt marts from the lake as it is now. They land on S3 as Parquet, in Glue."""
    import subprocess

    cat = lake.catalog()
    names = [*lake.TABLES, "customers_history"]
    locations = {n: cat.load_table(lake.table_name(n, CONSUMER)).metadata_location for n in names}
    cmd = ["dbt", "build", "--project-dir", config.DBT_DIR, "--profiles-dir", config.DBT_DIR]
    cmd += [
        "--target-path",
        "/tmp/dbt/target",
        "--log-path",
        "/tmp/dbt/logs",
        "--vars",
        json.dumps(locations),
    ]
    done = subprocess.run(cmd, capture_output=True, text=True)
    if done.returncode:
        raise RuntimeError(done.stdout[-3000:] + done.stderr[-1000:])
    with open("/tmp/dbt/target/run_results.json") as f:
        results = json.load(f)["results"]
    kinds = [r["unique_id"].split(".")[0] for r in results]
    return {"models": kinds.count("model"), "tests_passed": kinds.count("test")}


TASKS = {"maintain": maintain, "reconcile": reconcile, "marts": marts}


def handler(event: dict[str, Any], context: object) -> dict[str, Any]:
    """Lambda entry point: ``{"task": "maintain"}``."""
    return TASKS[event["task"]]()
