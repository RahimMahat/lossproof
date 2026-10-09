#!/usr/bin/env bash
# Stop everything and delete all state, including the containers Floci started.
set -uo pipefail
cd "$(dirname "$0")/.."

docker ps -aq --filter name=floci-aws | xargs -r docker rm -f >/dev/null
docker compose --profile pipeline down -v --remove-orphans
rm -f infra/terraform.tfstate infra/terraform.tfstate.backup .env
