#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT=/opt/weather-streaming/weather-streaming-bigdata
if [[ "$(realpath -m "$REPO_ROOT")" != "$REPO_ROOT" || ! -d "$REPO_ROOT" ]]; then
  echo "Expected Azure repository path does not exist: $REPO_ROOT" >&2
  exit 2
fi
if [[ $EUID -ne 0 ]]; then
  echo "Run this installer through sudo. It only installs systemd units and permissions." >&2
  exit 2
fi

DEPLOY_USER=${SUDO_USER:-$(stat -c '%U' "$REPO_ROOT")}
if [[ -z "$DEPLOY_USER" || "$DEPLOY_USER" == root ]]; then
  echo "Refusing to run the T2H tools as root; set SUDO_USER to the repository owner." >&2
  exit 2
fi
DEPLOY_GROUP=$(id -gn "$DEPLOY_USER")
DEPLOY_GID=$(id -g "$DEPLOY_USER")
STATE_DIR="$REPO_ROOT/data/runtime/prospective-live-t2h-168h-v1"
install -d -o "$DEPLOY_USER" -g "$DEPLOY_GROUP" -m 2770 "$STATE_DIR"

if ! getent group docker >/dev/null; then
  echo "The docker group is missing; add the deployment user to the Docker group before installing the watchdog." >&2
  exit 2
fi
if ! id -nG "$DEPLOY_USER" | tr ' ' '\n' | grep -qx docker; then
  echo "Deployment user '$DEPLOY_USER' must be in the docker group for watchdog/recovery." >&2
  exit 2
fi

env_tmp=$(mktemp)
printf 'WEATHER_RUNTIME_GID=%s\n' "$DEPLOY_GID" > "$env_tmp"
install -o root -g "$DEPLOY_GROUP" -m 0640 "$env_tmp" /etc/weather-streaming-t2h.env
rm -f "$env_tmp"

render_unit() {
  local input=$1 output=$2
  sed \
    -e "s|@REPO_ROOT@|$REPO_ROOT|g" \
    -e "s|@DEPLOY_USER@|$DEPLOY_USER|g" \
    -e "s|@DEPLOY_GROUP@|$DEPLOY_GROUP|g" \
    "$input" > "$output"
  chown root:root "$output"
  chmod 0644 "$output"
}

render_unit "$REPO_ROOT/ops/azure_t2h/systemd/weather-t2h-resume.service.in" /etc/systemd/system/weather-t2h-resume.service
render_unit "$REPO_ROOT/ops/azure_t2h/systemd/weather-t2h-watchdog.service.in" /etc/systemd/system/weather-t2h-watchdog.service
install -o root -g root -m 0644 "$REPO_ROOT/ops/azure_t2h/systemd/weather-t2h-watchdog.timer" /etc/systemd/system/weather-t2h-watchdog.timer

systemctl daemon-reload
systemctl enable weather-t2h-resume.service
systemctl enable --now weather-t2h-watchdog.timer
echo "Installed T2H resume-on-boot and five-minute watchdog units for $DEPLOY_USER. No cohort was started."
