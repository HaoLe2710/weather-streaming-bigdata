#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT=/opt/weather-streaming/weather-streaming-bigdata
REF=${1:-deploy/azure-phase17}
APPLY=${2:-false}
FRESH_READINESS=${3:-false}

if [[ ! "$REF" =~ ^[A-Za-z0-9][A-Za-z0-9._/-]{0,127}$ || "$REF" == *..* || "$REF" == */ ]]; then
  echo "Invalid Git ref." >&2
  exit 2
fi
if [[ "$APPLY" != true && "$APPLY" != false ]] || [[ "$FRESH_READINESS" != true && "$FRESH_READINESS" != false ]]; then
  echo "Internal apply/readiness flags must be true or false." >&2
  exit 2
fi
if [[ "$FRESH_READINESS" == true && "$APPLY" != true ]]; then
  echo "Fresh Formal Readiness requires the explicit Apply flag." >&2
  exit 2
fi
cd "$REPO_ROOT"

AZURE_COMPOSE_FILE='docker-compose.yml:compose.azure.yaml:compose.azure.resources.yaml'
if [[ -n "${COMPOSE_FILE:-}" && "$COMPOSE_FILE" != "$AZURE_COMPOSE_FILE" ]]; then
  echo "COMPOSE_FILE is not the required Azure stack: $COMPOSE_FILE" >&2
  exit 3
fi
export COMPOSE_FILE="$AZURE_COMPOSE_FILE"
export WEATHER_AZURE_RUNTIME=1
for compose_file in docker-compose.yml compose.azure.yaml compose.azure.resources.yaml; do
  if [[ ! -f "$compose_file" ]]; then
    echo "Required Azure Compose file is missing: $REPO_ROOT/$compose_file. Refusing fallback." >&2
    exit 3
  fi
done

if [[ "$APPLY" == true ]]; then
  if [[ "$(git branch --show-current)" != "$REF" ]]; then
    echo "Remote checkout branch must exactly match requested ref '$REF'. No branch switch was attempted." >&2
    exit 3
  fi
  if ! git diff --quiet || ! git diff --cached --quiet; then
    echo "Tracked Azure working-tree changes exist; refusing to deploy over them." >&2
    exit 3
  fi
  if [[ -f data/runtime/prospective-live-t2h-168h-v1/active_run.json ]]; then
    active_run=$(python3 - <<'PY'
import json
from pathlib import Path
p = Path("data/runtime/prospective-live-t2h-168h-v1/active_run.json")
try:
    value = json.loads(p.read_text(encoding="utf-8"))
    print(value.get("run_id", ""))
except Exception:
    print("INVALID")
PY
)
    if [[ -z "$active_run" || "$active_run" == INVALID ]]; then
      echo "active_run.json is invalid; refusing deployment." >&2
      exit 3
    fi
    if [[ ! "$active_run" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$ || "$active_run" == . || "$active_run" == .. ]]; then
      echo "active_run.json contains an unsafe run ID; refusing deployment." >&2
      exit 3
    fi
    active_status=$(python3 - "$active_run" <<'PY'
import json, sys
from pathlib import Path
path = Path("data/runtime/prospective-live-t2h-168h-v1") / sys.argv[1] / "cohort_status.json"
try:
    print(json.loads(path.read_text(encoding="utf-8")).get("status", "UNKNOWN"))
except Exception:
    print("UNKNOWN")
PY
)
    if [[ "$active_status" != FINALIZED ]]; then
      echo "An official cohort is active or its state is unverifiable ($active_run: $active_status). Refusing deployment." >&2
      exit 3
    fi
  fi

  git fetch --no-tags origin "$REF"
  fetched_ref=$(git rev-parse FETCH_HEAD)
  changed_paths=$(git diff --name-only HEAD "$fetched_ref")
  if grep -Eiq '(^|/)([^/]*azure[^/]*compose[^/]*|compose[^/]*azure[^/]*)\.(yml|yaml)$|(^|/)(docker-compose|compose)\.override\.(yml|yaml)$' <<<"$changed_paths"; then
    echo "Fetched change set touches an Azure-local Compose overlay; refusing fast-forward." >&2
    grep -Ei 'azure.*compose|compose.*azure|(docker-compose|compose)\.override' <<<"$changed_paths" >&2 || true
    exit 3
  fi
  git merge --ff-only "$fetched_ref"
fi

if [[ ! -x ops/azure_t2h/deploy-azure.sh ]]; then
  echo "Azure deployment tools are missing. Merge the tools into '$REF' first." >&2
  exit 4
fi

args=(--preflight)
if [[ "$APPLY" == true ]]; then args=(--apply); fi
if [[ "$FRESH_READINESS" == true ]]; then args+=(--fresh-readiness); fi
exec bash ops/azure_t2h/deploy-azure.sh "${args[@]}"
