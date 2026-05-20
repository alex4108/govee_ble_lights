#!/usr/bin/env bash
# Tail HA logs for govee-ble-lights rewrite/heartbeat/drift activity.
#
# Bumps the integration's logger to INFO at start so rewrite + heartbeat
# lines surface (default is WARNING). Restores to WARNING on Ctrl-C.
#
# Usage:
#     bash scripts/watch_heartbeat.sh             # tail forever, --since 1m
#     bash scripts/watch_heartbeat.sh --since 1h  # backfill last hour
#
# Lines you should see (each bulb, every _COLOR_HEARTBEAT_S = 600s while on):
#     INFO  govee-ble-lights: DB:E6:46:46:32:47 color heartbeat #5 ok (rgb=... bright=...)
# Right after a turn_on with color:
#     INFO  govee-ble-lights: <mac> color rewrite #1 ok (...)  (at +15s)
#     INFO  govee-ble-lights: <mac> color rewrite #2 ok (...)  (at +60s cumulative)
#     INFO  govee-ble-lights: <mac> color rewrite #3 ok (...)  (at +150s)
#     INFO  govee-ble-lights: <mac> color rewrite #4 ok (...)  (at +330s)
#     (then heartbeat #5 at +930s and every +600s after)
# On drift:
#     WARN  govee-ble-lights: <mac> drift detected (advert=..., intent=...); arming retry worker

set -euo pipefail

HOMELAB="${HOMELAB:-$HOME/repos/alex4108/homelab}"
# shellcheck disable=SC1091
source "$HOMELAB/util/load-secrets.sh"

: "${HA_SSH_HOST:?HA_SSH_HOST not set in secrets}"
: "${HA_SSH_PASSWORD:?HA_SSH_PASSWORD not set in secrets}"
: "${HA_ACCESS_TOKEN:?HA_ACCESS_TOKEN not set in secrets}"

SINCE="${1:-1m}"
if [[ "$1" == "--since" && -n "${2:-}" ]]; then
    SINCE="$2"
fi

LOGGER_TARGET="custom_components.govee-ble-lights.light"

set_logger_level() {
    local level="$1"
    curl -fsS -o /dev/null \
        -X POST \
        -H "Authorization: Bearer $HA_ACCESS_TOKEN" \
        -H 'Content-Type: application/json' \
        -d "{\"$LOGGER_TARGET\":\"$level\"}" \
        "http://${HA_SSH_HOST}:8123/api/services/logger/set_level" \
        && echo "  logger.$LOGGER_TARGET → $level"
}

cleanup() {
    echo
    echo "Restoring logger to WARNING..."
    set_logger_level warning || true
}
trap cleanup EXIT INT TERM

echo "Bumping HA logger to INFO for $LOGGER_TARGET..."
set_logger_level info

PATTERN='govee-ble-lights:.*(color rewrite|color heartbeat|drift detected|pending|failed)'

echo
echo "Tailing homeassistant container --since $SINCE for: $PATTERN"
echo "Ctrl-C to exit (logger will be restored to WARNING)."
echo

sshpass -p "$HA_SSH_PASSWORD" ssh -o StrictHostKeyChecking=no -o LogLevel=ERROR \
    "root@${HA_SSH_HOST}" \
    "docker logs -f --since $SINCE homeassistant 2>&1" \
  | grep --line-buffered -E "$PATTERN"
