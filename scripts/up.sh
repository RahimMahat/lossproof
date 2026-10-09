#!/usr/bin/env bash
# Build the whole platform from nothing: Floci, the Terraform resources, Debezium, the source schema.
set -euo pipefail
cd "$(dirname "$0")/.."

docker compose up -d floci
until curl -sf -m 3 http://localhost:4566/_floci/health >/dev/null; do sleep 1; done

terraform -chdir=infra init -input=false >/dev/null
terraform -chdir=infra apply -auto-approve -input=false

# Floci runs RDS as a Postgres container named after the instance's resource id.
pg=$(docker ps --format '{{.Names}}' --filter "name=floci-aws-rds-$(terraform -chdir=infra output -raw source_resource_id)")
printf 'LOSSPROOF_KAFKA=%s\nLOSSPROOF_PG_HOST=%s\n' "$(terraform -chdir=infra output -raw kafka_bootstrap)" "$pg" > .env

# Floci accepts the rds.logical_replication parameter but does not apply it (docs/adr/001).
if [ "$(docker exec "$pg" psql -U lp -d shop -Atc 'show wal_level')" != logical ]; then
  docker exec "$pg" psql -U lp -d shop -c 'alter system set wal_level = logical'
  docker restart "$pg" >/dev/null
  until docker exec "$pg" pg_isready -U lp -d shop >/dev/null 2>&1; do sleep 1; done
fi

docker compose --profile pipeline up -d --build connect
until docker compose exec -T connect curl -sf http://localhost:8083/connectors >/dev/null 2>&1; do sleep 2; done
docker compose run --rm app source init
