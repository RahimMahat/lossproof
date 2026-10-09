# ADR 001: What Floci can carry

**Status:** accepted · **Date:** 2026-10-09 · Floci 2.2.0, Terraform AWS provider 6.x, pyiceberg

## Question

The plan puts a CDC lakehouse on emulated AWS. Six parts were unproven. Each was tested with
`spike/iceberg_glue.py` and `spike/aws_services.py` before anything else was built.

## Results

| Part | Result | Consequence |
|---|---|---|
| Terraform (S3, Glue, SQS) | Works | `skip_requesting_account_id = true` is required, so Glue resources need an explicit `catalog_id`. |
| Iceberg through the Glue catalogue (pyiceberg) | Works: create, append, delete, overwrite, schema read, custom snapshot properties | No fallback catalogue needed. Consumer offsets can live in the snapshot summary. |
| Athena reading the Iceberg table | Works (Floci runs DuckDB) | Athena can query the lake directly, not only the marts. |
| Lambda (zip, python3.12) and Step Functions → Lambda | Works | Maintenance tasks are plain Lambdas. |
| MSK | Works (Floci runs Redpanda) | The bootstrap address is a container IP. Clients must be on the Docker network. |
| RDS Postgres | Works, real Postgres 16 | See below. |

## RDS and logical replication

- The `rds.logical_replication` parameter group setting is accepted but not applied.
  `ALTER SYSTEM SET wal_level = logical` followed by `RebootDBInstance` does apply, and data
  survives the reboot.
- The RDS endpoint is a proxy on the Floci container (port 7001). It passes ordinary queries but
  not the replication protocol. Debezium connects to the Postgres container directly on 5432.

## Decisions

1. Use Floci for every AWS service in the plan. No fallbacks are needed.
2. The generator, consumer and Debezium run as containers on the Compose network, because MSK
   and the replication port are not reachable from the host. A crash is then a real
   `docker kill`, which suits the chaos harness.
3. Not tested: concurrent writers to one Iceberg table. The design has one writer per table.
