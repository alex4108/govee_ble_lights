#!/usr/bin/env bash
# Deploy custom_components/govee-ble-lights/light.py from the current branch
# onto the production HA box, with timestamped backup + `ha core check` gate
# + restart + /api/ healthcheck.
#
# Usage (from the fork root, on the branch you want deployed):
#     bash scripts/deploy_to_ha.sh
#
# Requires the homelab repo for SSH credentials (HA_SSH_HOST, HA_SSH_PASSWORD,
# HA_ACCESS_TOKEN). Override location with HOMELAB=... if not at the default.

set -euo pipefail

HOMELAB="${HOMELAB:-$HOME/repos/alex4108/homelab}"
if [[ ! -f "$HOMELAB/util/load-secrets.sh" ]]; then
    echo "homelab repo not found at $HOMELAB — set HOMELAB env var" >&2
    exit 1
fi
# shellcheck disable=SC1091
source "$HOMELAB/util/load-secrets.sh"

: "${HA_SSH_HOST:?HA_SSH_HOST not set in secrets}"
: "${HA_SSH_PASSWORD:?HA_SSH_PASSWORD not set in secrets}"
: "${HA_ACCESS_TOKEN:?HA_ACCESS_TOKEN not set in secrets}"

REPO="$(git rev-parse --show-toplevel)"
SRC="$REPO/custom_components/govee-ble-lights/light.py"
[[ -f "$SRC" ]] || { echo "source missing: $SRC" >&2; exit 1; }

BRANCH="$(git rev-parse --abbrev-ref HEAD)"
SHA="$(git rev-parse --short HEAD)"
TAG="${BRANCH//\//-}-$SHA"
TS="$(date -u +%Y%m%d_%H%M%S)"

REMOTE_PATH="/config/custom_components/govee-ble-lights/light.py"
BACKUP_PATH="${REMOTE_PATH}.backup.${TS}.${TAG}"

ssh_ha() {
    sshpass -p "$HA_SSH_PASSWORD" ssh -o StrictHostKeyChecking=no \
        -o LogLevel=ERROR "root@${HA_SSH_HOST}" "$@"
}
scp_ha() {
    sshpass -p "$HA_SSH_PASSWORD" scp -o StrictHostKeyChecking=no \
        -o LogLevel=ERROR "$@"
}

echo "branch=$BRANCH sha=$SHA host=$HA_SSH_HOST"
echo

echo "[1/6] backup remote light.py → $BACKUP_PATH"
ssh_ha "cp '$REMOTE_PATH' '$BACKUP_PATH' && ls -la '$BACKUP_PATH'"

echo
echo "[2/6] scp local light.py → HA"
scp_ha "$SRC" "root@${HA_SSH_HOST}:${REMOTE_PATH}" >/dev/null

echo "[3/6] md5 round-trip check"
LOCAL_MD5="$(md5sum "$SRC" | awk '{print $1}')"
REMOTE_MD5="$(ssh_ha "md5sum '$REMOTE_PATH'" | awk '{print $1}')"
if [[ "$LOCAL_MD5" != "$REMOTE_MD5" ]]; then
    echo "  md5 mismatch local=$LOCAL_MD5 remote=$REMOTE_MD5 — rolling back" >&2
    ssh_ha "cp '$BACKUP_PATH' '$REMOTE_PATH'"
    exit 1
fi
echo "  md5=$LOCAL_MD5"

echo
echo "[4/6] ha core check (must pass)"
if ! ssh_ha "ha core check"; then
    echo "  FAILED — rolling back to $BACKUP_PATH" >&2
    ssh_ha "cp '$BACKUP_PATH' '$REMOTE_PATH'"
    exit 1
fi

echo
echo "[5/6] ha core restart"
PRE_STARTED="$(ssh_ha "docker inspect homeassistant --format '{{.State.StartedAt}}'")"
echo "  pre-restart StartedAt=$PRE_STARTED"
ssh_ha "ha core restart" || true   # `ha core restart` may close the connection
echo "  restart issued"

echo
echo "[6/6] wait for HA to come back (/api/ 200)"
DEADLINE=$((SECONDS + 300))
while (( SECONDS < DEADLINE )); do
    sleep 5
    NEW_STARTED="$(ssh_ha "docker inspect homeassistant --format '{{.State.StartedAt}}'" 2>/dev/null || echo '')"
    if [[ -n "$NEW_STARTED" && "$NEW_STARTED" != "$PRE_STARTED" ]]; then
        if curl -fsS -o /dev/null --max-time 5 \
            -H "Authorization: Bearer $HA_ACCESS_TOKEN" \
            "http://${HA_SSH_HOST}:8123/api/"; then
            echo "  HA back up at $NEW_STARTED"
            echo
            echo "Deployed: branch=$BRANCH sha=$SHA md5=$LOCAL_MD5"
            echo "Backup on HA: $BACKUP_PATH"
            echo
            echo "Watch the rewrite/heartbeat log lines with:"
            echo "    bash scripts/watch_heartbeat.sh"
            exit 0
        fi
    fi
done
echo "HA did not return /api/ 200 within 300s — investigate manually" >&2
echo "  rollback: ssh root@${HA_SSH_HOST} \"cp '$BACKUP_PATH' '$REMOTE_PATH' && ha core restart\"" >&2
exit 1
