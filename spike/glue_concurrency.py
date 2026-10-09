"""Spike: does Floci's Glue reject a commit made from a stale view of an Iceberg table?"""
import sys

import pyarrow as pa
from pyiceberg.exceptions import CommitFailedException

sys.path.insert(0, "src")
from lossproof import lake  # noqa: E402

cat = lake.catalog()
name = "lossproof.spike_concurrency"
schema = pa.schema([("id", pa.int64())])
try:
    cat.drop_table(name)
except Exception:
    pass
cat.create_table(name, schema=schema)
a, b = cat.load_table(name), cat.load_table(name)
a.append(pa.table({"id": [1]}, schema=schema))
try:
    b.append(pa.table({"id": [2]}, schema=schema))  # b still holds the empty table's metadata
    print("stale append accepted; rows now:", sorted(cat.load_table(name).scan().to_arrow()["id"].to_pylist()))
except CommitFailedException as e:
    print("stale append rejected:", str(e)[:150])
