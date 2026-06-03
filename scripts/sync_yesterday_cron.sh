#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG_DIR="${ROOT_DIR}/tmp/cron_logs"
LOCK_FILE="/tmp/wb_sync_recent.lock"
CONTAINER_NAME="wb-sync-web"
SYNC_TIMEOUT_SECONDS="${WB_SYNC_TIMEOUT_SECONDS:-10800}"

mkdir -p "$LOG_DIR"

DATE_FROM="$(date -d '2 days ago' +%F)"
YESTERDAY="$(date -d 'yesterday' +%F)"
LOG_FILE="${LOG_DIR}/sync_recent_${DATE_FROM}_${YESTERDAY}.log"

exec 9>"$LOCK_FILE"
if ! flock -n 9; then
  {
    echo "[$(date -Is)] Another sync is already running, skipping ${DATE_FROM}..${YESTERDAY}."
  } >> "$LOG_FILE" 2>&1
  exit 0
fi

run_sync() {
  local label="$1"
  shift
  echo "[$(date -Is)] ${label}: docker exec $*"
  timeout "$SYNC_TIMEOUT_SECONDS" docker exec -e SQLITE_DB_PATH=/app/data/cabs/ewb.db "$CONTAINER_NAME" python -u -m wb_gsheets.main "$@"
}

{
  echo "[$(date -Is)] Starting WB sync for ${DATE_FROM}..${YESTERDAY}"
  cd "$ROOT_DIR"
  docker compose up -d wb-sync-web

  if run_sync "full attempt 1" --date-from "$DATE_FROM" --date-to "$YESTERDAY"; then
    echo "[$(date -Is)] Finished WB sync for ${DATE_FROM}..${YESTERDAY}"
    exit 0
  fi

  echo "[$(date -Is)] Full attempt 1 failed, retrying once."
  if run_sync "full attempt 2" --date-from "$DATE_FROM" --date-to "$YESTERDAY"; then
    echo "[$(date -Is)] Finished WB sync for ${DATE_FROM}..${YESTERDAY} after retry"
    exit 0
  fi

  echo "[$(date -Is)] Full sync failed twice. Running fallback without ads."
  run_sync "fallback skip ads" --date-from "$DATE_FROM" --date-to "$YESTERDAY" --skip-ads
  echo "[$(date -Is)] Finished fallback WB sync for ${DATE_FROM}..${YESTERDAY}"
} >> "$LOG_FILE" 2>&1
