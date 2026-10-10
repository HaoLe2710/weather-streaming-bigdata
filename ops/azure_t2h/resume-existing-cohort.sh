#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT=/opt/weather-streaming/weather-streaming-bigdata
STATE_ROOT="$REPO_ROOT/data/runtime/prospective-live-t2h-168h-v1"
cd "$REPO_ROOT"
if [[ -f /etc/weather-streaming-t2h.env ]]; then
  set -a
  # shellcheck disable=SC1091
  source /etc/weather-streaming-t2h.env
  set +a
else
  export WEATHER_RUNTIME_GID="$(id -g)"
fi

if [[ ! -f "$STATE_ROOT/active_run.json" ]]; then
  echo 'No official cohort is active; nothing to resume.'
  exit 0
fi
run_id=$(.venv/bin/python - "$STATE_ROOT/active_run.json" <<'PY'
import json, re, sys
from pathlib import Path
value = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
run_id = value.get("run_id")
if not isinstance(run_id, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", run_id) or run_id in {".", ".."}:
    raise SystemExit("active_run.json has an unsafe run_id")
print(run_id)
PY
)
request="$STATE_ROOT/$run_id/start_request.json"
if [[ ! -f "$request" ]]; then
  echo "Active run request is missing; refusing to start or reconstruct a cohort: $run_id" >&2
  exit 3
fi
.venv/bin/python - "$request" "$run_id" <<'PY'
import json, sys
from pathlib import Path
request = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
if request.get("run_id") != sys.argv[2] or request.get("cohort_protocol_id") != "T2H_LIVE_PROSPECTIVE_168H_V1":
    raise SystemExit("Run request does not match its frozen run_id/protocol")
PY
status=$(.venv/bin/python - "$STATE_ROOT/$run_id/cohort_status.json" <<'PY'
import json, sys
from pathlib import Path
try:
    print(json.loads(Path(sys.argv[1]).read_text(encoding="utf-8")).get("status", "WAITING_FOR_VALID_CYCLE"))
except FileNotFoundError:
    print("WAITING_FOR_VALID_CYCLE")
PY
)
if [[ "$status" == FINALIZED ]]; then
  echo "Cohort $run_id is already finalized; nothing to resume."
  exit 0
fi
exec .venv/bin/python -m validation.prospective_t2h --state-root "$STATE_ROOT" resume
