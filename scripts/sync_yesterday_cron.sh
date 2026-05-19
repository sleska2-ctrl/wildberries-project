#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG_DIR="${ROOT_DIR}/tmp/cron_logs"
LOCK_FILE="/tmp/wb_sync_yesterday.lock"
CONTAINER_NAME="wb-sync-web"

mkdir -p "$LOG_DIR"

YESTERDAY="$(date -d 'yesterday' +%F)"
LOG_FILE="${LOG_DIR}/sync_yesterday_${YESTERDAY}.log"

exec 9>"$LOCK_FILE"
if ! flock -n 9; then
  {
    echo "[$(date -Is)] Another sync is already running, skipping ${YESTERDAY}."
  } >> "$LOG_FILE" 2>&1
  exit 0
fi

{
  echo "[$(date -Is)] Starting WB sync for ${YESTERDAY}"
  cd "$ROOT_DIR"
  docker compose up -d wb-sync-web
  docker exec "$CONTAINER_NAME" python -m wb_gsheets.main --date-from "$YESTERDAY" --date-to "$YESTERDAY"
  echo "[$(date -Is)] Finished WB sync for ${YESTERDAY}"
} >> "$LOG_FILE" 2>&1
