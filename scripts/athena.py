"""Run one SQL statement through Athena and print the rows.

Run on the host: uv run python scripts/athena.py "select count(*) from fct_orders"
"""

import sys
import time

import boto3

athena = boto3.client(
    "athena",
    endpoint_url="http://localhost:4566",
    region_name="us-east-1",
    aws_access_key_id="test",
    aws_secret_access_key="test",
)
query = athena.start_query_execution(
    QueryString=sys.argv[1],
    QueryExecutionContext={"Database": "lossproof"},
    ResultConfiguration={"OutputLocation": "s3://lossproof-lake/athena/"},
)["QueryExecutionId"]
while (status := athena.get_query_execution(QueryExecutionId=query)["QueryExecution"]["Status"])["State"] in (
    "QUEUED",
    "RUNNING",
):
    time.sleep(0.5)
if status["State"] != "SUCCEEDED":
    sys.exit(f"{status['State']}: {status.get('StateChangeReason')}")
for row in athena.get_query_results(QueryExecutionId=query)["ResultSet"]["Rows"]:
    print("\t".join(d.get("VarCharValue", "") for d in row["Data"]))
