# ADR 004: The consumer and the maintenance task write the same tables

**Status:** accepted · **Date:** 2026-10-09

## Problem

The consumer rewrites data files on every batch, so tables fragment (about 15 files after
10,000 transactions). A scheduled Lambda compacts them and expires old snapshots. That makes two
writers on one table, and the consumer's commit is a delete plus an append, which can't simply
be replayed on top of someone else's commit.

## What was observed

Running compaction in a loop while the consumer wrote:

- The Iceberg client refuses a commit that conflicts with files added since it started
  (`ValidationException`), and Floci's Glue rejects an update made against an old table version
  (`CommitFailedException: Glue detected concurrent update`). Neither writer silently overwrote
  the other.
- The first version of the maintenance task did not expect this and crashed.

## Decision

- **Maintenance loses politely.** If its commit is refused, it skips that table and reports
  "the consumer committed first". Compaction is optional work; the next run will do it.
- **The consumer starts the batch again.** On a refused commit it re-subscribes, which re-reads
  the offsets stored in the lake (ADR 002) and applies the batch to the table as it now is.

With 13 maintenance runs during a 20,000-transaction workload, the correct consumer's tables
matched the source exactly.

## Consequences

- No lock service and no coordination between the two processes.
- Compaction rewrites a whole table in memory. That is fine at this scale and would need to
  become per-partition for large tables.
- The history table (`customers_history`) is append-only and is not compacted.
