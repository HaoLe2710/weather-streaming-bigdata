#!/usr/bin/env bash
set -u

label=$1
shift
log_file=$(mktemp)

set +e
python -m pytest "$@" >"$log_file" 2>&1
status=$?
set -e

cat "$log_file"
if (( status != 0 )); then
  summary=$(tail -n 25 "$log_file" | tr -d '\r' | tr '\n' ' ' | sed 's/%/%25/g')
  echo "::error title=${label} failed::${summary:0:5000}"
fi

rm -f "$log_file"
exit "$status"
