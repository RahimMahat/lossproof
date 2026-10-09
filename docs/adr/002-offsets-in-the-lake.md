# ADR 002: Kafka offsets live in the lake

**Status:** accepted · **Date:** 2026-10-09

## Problem

A consumer does two things per batch: it writes rows to the lake and it records how far it has
read. If those are two separate writes, a crash between them leaves them disagreeing.

- Offset first, data second: the batch is lost. Rows go missing or stay stale.
- Data first, offset second: the batch is applied again. That is safe only if applying is
  idempotent.

The careless consumer lets the Kafka client commit offsets on a timer, so either order can occur.
Killed every 8 to 14 seconds under load, it ended with 504, 439 and 0 wrong rows on three
seeds. The damage depends on where each kill lands.

## Decision

Each lake table stores the offsets of its topic in a table property (`lossproof.offsets`),
written in the same Iceberg commit as the batch's rows. On start, and whenever partitions are
assigned, the consumer reads those properties and seeks to them. Kafka's own committed offsets
are still updated after each batch, but only so that lag is visible to ordinary Kafka tooling.
Nothing reads them back.

One Iceberg commit is atomic: the catalogue swaps one metadata pointer. So the rows and the
position they correspond to can't disagree, whatever happens to the process.

## Consequences

- Exactly-once into the lake, without Kafka transactions.
- The four tables commit separately. That is fine because each table has its own topic and
  its own offsets; nothing needs the four to move together.
- A batch that contains only junk still commits, so its offsets advance past the junk.
- The dead-letter queue is at-least-once. A crash after a bad message is quarantined but before
  its batch commits sends it again on replay (40 entries for 20 bad messages in the combined
  run). Each entry carries its topic, partition and offset, so repeats are identifiable.
- A table property was chosen over the snapshot summary because it can be set in a commit that
  adds no data.
