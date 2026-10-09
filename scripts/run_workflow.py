"""Start the nightly workflow once and print how it ended.

Run on the host: uv run python scripts/run_workflow.py
"""

import json
import time

import boto3

kw = dict(
    endpoint_url="http://localhost:4566",
    region_name="us-east-1",
    aws_access_key_id="test",
    aws_secret_access_key="test",
)
sfn = boto3.client("stepfunctions", **kw)
arn = next(
    m["stateMachineArn"]
    for m in sfn.list_state_machines()["stateMachines"]
    if m["name"] == "lossproof-nightly"
)
ex = sfn.start_execution(stateMachineArn=arn)["executionArn"]
while (r := sfn.describe_execution(executionArn=ex))["status"] == "RUNNING":
    time.sleep(2)
print(r["status"], r.get("error", ""), r.get("cause", "")[:1500])
print(json.dumps(json.loads(r.get("output") or "{}"), indent=1))
cw = boto3.client("cloudwatch", **kw)
print(sorted({m["MetricName"] for m in cw.list_metrics(Namespace="Lossproof")["Metrics"]}))
print(boto3.client("s3", **kw).list_objects_v2(Bucket="lossproof-lake", Prefix="reports/").get("KeyCount"))
