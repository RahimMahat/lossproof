# The story

## The claim

A change-data-capture pipeline that matches its source on a quiet day can be badly wrong after
an ordinary week: a restart, a schema change, a connector that had to be rebuilt. I wanted to
count the wrong rows instead of asserting that they exist.

## What I built

1. **A source and a stream.** A seeded generator runs an order workload (inserts, updates,
   deletes, cascades) against Postgres. Debezium turns the write-ahead log into Kafka topics.
2. **A careless consumer, on purpose.** Auto-committed offsets, changes applied in arrival order,
   two commits per batch, a fixed schema, errors logged and skipped. It writes Iceberg tables.
3. **A reconciler.** It compares every source row with the lake, key by key, and counts rows that
   are missing, stale, duplicated or still present after deletion. I built it before the second
   consumer, because it is the measurement everything else is judged by.
4. **A chaos harness.** Seeded scenarios that kill consumers, rewind them, change the schema, drop
   the replication slot, inject junk and run compaction, with both consumers on the same stream.
5. **A correct consumer.** Offsets stored in the same Iceberg commit as the data, merge by key and
   log position, tombstones, schema evolution, a dead-letter queue, and a purge after a snapshot.
6. **A platform.** All of it on emulated AWS, created by Terraform: a nightly Step Functions
   workflow that compacts the lake, reconciles it, and only then builds dbt marts from it.

## What I found

- **With every fault in one run, 69% of the careless consumer's lake was wrong** (53,785 of
  77,962 rows after 100,000 transactions). The correct consumer's lake matched exactly.
- **The fault I expected to matter most did nothing.** Redelivering every message from the start,
  twice, left the careless consumer with 0 wrong rows. Replaying changes in order is harmless.
- **One `ALTER TABLE` did the most damage** (8,319 of 16,382 rows). Widening a column made writes
  fail after the old rows had already been deleted, because the batch was two commits.
- **A lost replication slot loses deletes silently** (991 ghost rows). Nothing in the stream is
  wrong; the deletes were simply never sent.
- **Crash damage is a matter of luck.** The careless consumer ended with 504, 439 and 0 wrong rows
  on three seeds. A pipeline like this can pass a crash test and still be unsafe.

## Things that were harder than expected

- **My first crash test proved nothing.** The careless consumer survived eight kills with 0 wrong
  rows, because every kill landed while it was idle. Kills only matter under load, so the harness
  now kills continuously while the workload runs.
- **A failed run looked like a drained one.** The emulated Kafka ran out of room for new topics
  and Debezium's task died. The stream went quiet, the harness took quiet for "finished", and both
  consumers scored 16,328 wrong of 16,323. The harness now deletes old topics and checks the
  connector before it trusts a result.
- **Kafka Connect remembers a connector by name.** Re-creating a connector with the same name
  after a reset made it resume from a position in a slot that no longer existed. Connector and
  slot names now carry the time.
- **Two writers on one table.** The first compaction task crashed when the consumer committed
  ahead of it. Iceberg and the catalogue both refuse the stale commit, which is the safe outcome,
  but each writer needs a plan for losing: compaction skips the table, the consumer starts the
  batch again from the offsets in the lake.
- **Deletes need a log position too.** Removing a deleted row leaves nothing to compare a late
  change against. Keeping a tombstone solved that and also made the snapshot purge a single rule:
  anything older than the snapshot is gone.
- **Two marts read each other's file.** dbt registered both Parquet files under the same Glue
  location, so each Athena table returned the other's rows as well. Each mart now has its own
  folder. The reconciler does not cover the marts; a dbt test compares the dimension's current
  versions with the lake instead.

## What I would do next

- Apply the Terraform to real AWS and replace the two emulator workarounds.
- Reconcile at a log position, so it can run against a live source.
- Repeat every scenario over many seeds and report the spread.
- Partition the large tables and compact per partition.
- Trim tombstones and history older than the longest possible replay.
- Handle renamed and retyped columns, which currently stop the consumer.
