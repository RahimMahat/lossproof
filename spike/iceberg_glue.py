"""M0 spike: does pyiceberg's Glue catalogue work against Floci? Prints PASS/FAIL per step."""
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import CommitFailedException

EP = "http://localhost:4566"
props = {
    "type": "glue", "glue.endpoint": EP, "glue.region": "us-east-1",
    "glue.access-key-id": "test", "glue.secret-access-key": "test",
    "s3.endpoint": EP, "s3.access-key-id": "test", "s3.secret-access-key": "test",
    "s3.region": "us-east-1", "s3.path-style-access": "true",
    "warehouse": "s3://lossproof-lake/warehouse",
}


def step(name, fn):
    try:
        print(f"PASS {name}: {fn()}")
    except Exception as e:  # spike: report and carry on
        print(f"FAIL {name}: {type(e).__name__}: {str(e)[:300]}")


cat = load_catalog("floci", **props)
schema = pa.schema([pa.field("id", pa.int64(), nullable=False), pa.field("v", pa.string())])
rows = lambda ids, v: pa.table({"id": ids, "v": [v] * len(ids)}, schema=schema)  # noqa: E731

step("drop old", lambda: cat.drop_table("lossproof.spike"))
step("create", lambda: cat.create_table("lossproof.spike", schema=schema).metadata_location)
step("append", lambda: cat.load_table("lossproof.spike").append(rows([1, 2, 3], "a")))
step("delete", lambda: cat.load_table("lossproof.spike").delete("id = 2"))
step("overwrite", lambda: cat.load_table("lossproof.spike").overwrite(rows([3], "b"), overwrite_filter="id = 3"))
step("scan", lambda: cat.load_table("lossproof.spike").scan().to_arrow().sort_by("id").to_pylist())


def summary_props():
    t = cat.load_table("lossproof.spike")
    t.append(rows([9], "c"), snapshot_properties={"lossproof.offsets": '{"0": 42}'})
    return cat.load_table("lossproof.spike").current_snapshot().summary["lossproof.offsets"]


step("offsets in snapshot summary", summary_props)


def conflict():
    a, b = cat.load_table("lossproof.spike"), cat.load_table("lossproof.spike")
    a.update_schema().add_column("x", "string").commit() if False else a.append(rows([10], "d"))
    try:
        b.update_schema().add_column("extra", pa.string()).commit()
    except CommitFailedException:
        return "stale commit rejected"
    raise AssertionError("stale commit was accepted: no optimistic locking")


step("concurrent commit", conflict)
step("schema evolution", lambda: [f.name for f in cat.load_table("lossproof.spike").schema().fields])
print("metadata:", cat.load_table("lossproof.spike").metadata_location)
