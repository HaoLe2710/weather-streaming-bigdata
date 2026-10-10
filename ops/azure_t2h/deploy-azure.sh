#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT=/opt/weather-streaming/weather-streaming-bigdata
STATE_ROOT="$REPO_ROOT/data/runtime/prospective-live-t2h-168h-v1"
APPLY=false
FRESH_READINESS=false
for arg in "$@"; do
  case "$arg" in
    --preflight) ;;
    --apply) APPLY=true ;;
    --fresh-readiness) FRESH_READINESS=true ;;
    *) echo "Unknown argument: $arg" >&2; exit 2 ;;
  esac
done
if [[ "$FRESH_READINESS" == true && "$APPLY" != true ]]; then
  echo "Fresh Formal Readiness requires --apply." >&2
  exit 2
fi
cd "$REPO_ROOT"
if [[ -f /etc/weather-streaming-t2h.env ]]; then
  # Host-scoped numeric Compose group setting, installed by install-systemd.sh.
  set -a
  # shellcheck disable=SC1091
  source /etc/weather-streaming-t2h.env
  set +a
else
  export WEATHER_RUNTIME_GID="$(id -g)"
fi

if [[ "$APPLY" == true ]]; then
  if ! git diff --quiet || ! git diff --cached --quiet; then
    echo "Tracked Azure working-tree changes exist; refusing to install services." >&2
    exit 3
  fi
  if [[ -f "$STATE_ROOT/active_run.json" ]]; then
    active_run=$(python3 - "$STATE_ROOT/active_run.json" <<'PY'
import json, sys
try:
    print(json.load(open(sys.argv[1], encoding="utf-8")).get("run_id", ""))
except Exception:
    print("INVALID")
PY
)
    if [[ -z "$active_run" || "$active_run" == INVALID ]]; then
      echo "active_run.json is invalid; refusing systemd installation." >&2
      exit 3
    fi
    if [[ ! "$active_run" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$ || "$active_run" == . || "$active_run" == .. ]]; then
      echo "active_run.json contains an unsafe run ID; refusing systemd installation." >&2
      exit 3
    fi
    status=$(python3 - "$STATE_ROOT/$active_run/cohort_status.json" <<'PY'
import json, sys
try:
    print(json.load(open(sys.argv[1], encoding="utf-8")).get("status", "UNKNOWN"))
except Exception:
    print("UNKNOWN")
PY
)
    if [[ "$status" != FINALIZED ]]; then
      echo "An official cohort is active or its state is unverifiable ($active_run: $status)." >&2
      exit 3
    fi
  fi
fi
export WEATHER_RUNTIME_GID="$(id -g)"

echo 'Checking frozen model and feature contract...'
.venv/bin/python - <<'PY'
import json
from validation.prospective_t2h import _default_model_contract
expected = {
    "model_sha256": "bd5ee153b2709ac661557bdd11f8322b80de1264c65a27d1d6c79fbcf63ee66a",
    "feature_list_sha256": "20a5d2fb56d9b7231f4c43b39ad7a833298d76b1bfd0f127b2b251c57e5d7fd2",
}
report = _default_model_contract()
print(json.dumps(report, ensure_ascii=False, sort_keys=True))
if report.get("status") != "PASS" or any(report.get(key) != value for key, value in expected.items()):
    raise SystemExit("Frozen model/feature contract check failed.")
PY

echo 'Running repository preflight, tests, and Compose config validation...'
.venv/bin/python -m validation.prospective_t2h --state-root "$STATE_ROOT" preflight

if [[ "$APPLY" == true ]]; then
  sudo -n bash ops/azure_t2h/install-systemd.sh
fi

if [[ "$FRESH_READINESS" == true ]]; then
  echo 'Running a new isolated Fresh Formal Readiness cycle; this does not start the official cohort.'
  .venv/bin/python -m validation.prospective_t2h --state-root "$STATE_ROOT" readiness --wait-seconds 1200
  .venv/bin/python - "$STATE_ROOT/readiness.json" <<'PY'
import json, sys
from pathlib import Path
report = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
required = (
    report.get("status") == "PASS",
    report.get("cohort_protocol_id") == "T2H_LIVE_PROSPECTIVE_168H_V1",
    report.get("forecast_count") == 63,
    report.get("positive_lead_count") == 63,
    report.get("target_offset_violations") == 0,
    report.get("monitoring_status") == "PASS",
    report.get("provider_model") == "ecmwf_ifs",
    report.get("readiness_services_stopped") is True,
)
if not all(required):
    raise SystemExit("Fresh Formal Readiness evidence did not satisfy every blocking gate.")
PY
fi

echo 'Current prospective status (read-only):'
.venv/bin/python -m validation.prospective_t2h --state-root "$STATE_ROOT" status
echo 'Azure deployment checks completed. The official cohort was not started.'
