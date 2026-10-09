"""M0 spike: Athena over the Iceberg table, RDS Postgres logical replication, MSK, Lambda + Step Functions."""
import io
import json
import time
import zipfile

import boto3

kw = dict(endpoint_url="http://localhost:4566", region_name="us-east-1",
          aws_access_key_id="test", aws_secret_access_key="test")
c = lambda s: boto3.client(s, **kw)  # noqa: E731


def step(name, fn):
    try:
        print(f"PASS {name}: {fn()}", flush=True)
    except Exception as e:  # spike: report and carry on
        print(f"FAIL {name}: {type(e).__name__}: {str(e)[:400]}", flush=True)


def athena(sql):
    a = c("athena")
    q = a.start_query_execution(QueryString=sql, QueryExecutionContext={"Database": "lossproof"},
                                ResultConfiguration={"OutputLocation": "s3://lossproof-lake/athena/"})["QueryExecutionId"]
    for _ in range(120):
        s = a.get_query_execution(QueryExecutionId=q)["QueryExecution"]["Status"]
        if s["State"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(1)
    if s["State"] != "SUCCEEDED":
        raise RuntimeError(f"{s['State']}: {s.get('StateChangeReason')}")
    rows = a.get_query_results(QueryExecutionId=q)["ResultSet"]["Rows"]
    return [[d.get("VarCharValue") for d in r["Data"]] for r in rows]


def glue_table():
    t = c("glue").get_table(DatabaseName="lossproof", Name="spike")["Table"]
    return {k: t.get(k) for k in ("TableType", "Parameters")}


def rds():
    r = c("rds")
    try:
        r.create_db_parameter_group(DBParameterGroupName="lp-logical", DBParameterGroupFamily="postgres16",
                                    Description="logical replication")
        r.modify_db_parameter_group(DBParameterGroupName="lp-logical", Parameters=[
            {"ParameterName": "rds.logical_replication", "ParameterValue": "1", "ApplyMethod": "pending-reboot"}])
    except Exception as e:
        print("  param group:", str(e)[:200])
    try:
        r.create_db_instance(DBInstanceIdentifier="lp-src", Engine="postgres", DBInstanceClass="db.t3.micro",
                             MasterUsername="lp", MasterUserPassword="lossproof1", DBName="shop",
                             AllocatedStorage=5, DBParameterGroupName="lp-logical")
    except Exception as e:
        print("  create:", str(e)[:200])
    for _ in range(90):
        d = r.describe_db_instances(DBInstanceIdentifier="lp-src")["DBInstances"][0]
        if d["DBInstanceStatus"] == "available":
            break
        time.sleep(2)
    return d["DBInstanceStatus"], d.get("Endpoint")


def msk():
    k = c("kafka")
    arns = [x["ClusterArn"] for x in k.list_clusters()["ClusterInfoList"] if x["ClusterName"] == "lp-stream"]
    arn = arns[0] if arns else k.create_cluster(
        ClusterName="lp-stream", KafkaVersion="3.6.0", NumberOfBrokerNodes=1,
        BrokerNodeGroupInfo={"InstanceType": "kafka.t3.small", "ClientSubnets": ["subnet-1"]})["ClusterArn"]
    for _ in range(90):
        st = k.describe_cluster(ClusterArn=arn)["ClusterInfo"]["State"]
        if st == "ACTIVE":
            break
        time.sleep(2)
    return st, k.get_bootstrap_brokers(ClusterArn=arn)


def lam_sfn():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("h.py", "def handler(event, context):\n    return {'ok': True, 'n': event.get('n', 0) + 1}\n")
    lam = c("lambda")
    try:
        lam.create_function(FunctionName="lp-echo", Runtime="python3.12", Handler="h.handler",
                            Role="arn:aws:iam::000000000000:role/lp", Code={"ZipFile": buf.getvalue()}, Timeout=30)
    except lam.exceptions.ResourceConflictException:
        pass
    for _ in range(60):
        if lam.get_function(FunctionName="lp-echo")["Configuration"].get("State", "Active") == "Active":
            break
        time.sleep(1)
    arn = lam.get_function(FunctionName="lp-echo")["Configuration"]["FunctionArn"]
    sfn = c("stepfunctions")
    d = {"StartAt": "A", "States": {"A": {"Type": "Task", "Resource": arn, "Next": "B"},
                                    "B": {"Type": "Task", "Resource": arn, "End": True}}}
    sm = sfn.create_state_machine(name=f"lp-spike-{int(time.time())}", definition=json.dumps(d),
                                  roleArn="arn:aws:iam::000000000000:role/lp")["stateMachineArn"]
    ex = sfn.start_execution(stateMachineArn=sm, input='{"n": 0}')["executionArn"]
    for _ in range(120):
        r = sfn.describe_execution(executionArn=ex)
        if r["status"] != "RUNNING":
            break
        time.sleep(1)
    return r["status"], r.get("output"), r.get("cause")


step("glue table as pyiceberg registered it", glue_table)
step("athena select iceberg table", lambda: athena("select * from spike order by id"))
step("lambda + step functions", lam_sfn)
step("msk", msk)
step("rds postgres", rds)
