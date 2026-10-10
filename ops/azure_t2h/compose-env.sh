#!/usr/bin/env bash
set -euo pipefail

set_weather_azure_compose_environment() {
  local repo_root=${1:?repository root is required}
  local expected='docker-compose.yml:compose.azure.yaml:compose.azure.resources.yaml'
  if [[ -n "${COMPOSE_FILE:-}" && "$COMPOSE_FILE" != "$expected" ]]; then
    echo "COMPOSE_FILE is not the required Azure stack: $COMPOSE_FILE" >&2
    return 2
  fi
  export COMPOSE_FILE="$expected"
  export WEATHER_AZURE_RUNTIME=1
  "$repo_root/.venv/bin/python" -m validation.azure_compose --repo-root "$repo_root"
}
