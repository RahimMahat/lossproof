# ADR 003: Merge by key and log position, keep deletes as tombstones

**Status:** accepted · **Date:** 2026-10-09

## Problem

Three things can make the lake disagree with the source even when no message is lost.

1. **A change arrives that is older than what the lake holds.** Kafka keeps order within a
   partition, but a restarted or duplicated connector can send old changes after new ones.
2. **A delete leaves nothing to compare against.** If the row is simply removed, a late, older
   change for the same key looks new and brings the row back.
3. **Deletes are missed entirely.** If the replication slot is lost, the new connector starts
   with a snapshot of the rows that exist now. Rows deleted while nothing was listening are not
   in the snapshot and no delete is ever sent. In the test this left the careless consumer with
   991 ghost rows out of 15,980.

## Decision

- Every lake row carries `_lsn`, the Postgres log position of the change that produced it.
  A change is applied only if its position is not older than the row's.
- A delete is stored as a tombstone: the row stays, with `_deleted = true` and the delete's
  position. Readers filter tombstones out.
- When a snapshot's final row arrives (Debezium marks it `last`), every row in every table
  whose position is older than the snapshot's is turned into a tombstone. The snapshot rewrote
  every row that still exists, so an older position means the row is gone.

The purge can run before all of a snapshot's rows have been read, because the tables are on
different topics. That is safe: a snapshot row carries the snapshot's position, which is not
older than the tombstone, so it restores the row. The table that carried the final marker
commits last, so a crash part-way through replays the marker and the purge runs again.

## Consequences

- Tombstones are never removed. A real deployment would drop them once older than the longest
  possible replay.
- "Not older" (rather than "newer") is deliberate. It lets a snapshot row undo a premature
  tombstone, and makes replaying the same change harmless.
- Plain redelivery turned out not to need any of this: rewinding the careless consumer to the
  start of every topic twice left it with 0 wrong rows, because replaying a contiguous stretch
  in order ends in the same state. The log position matters when order is broken, not when
  messages repeat.
