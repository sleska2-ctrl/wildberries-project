#!/usr/bin/env bash
set -euo pipefail

export TZ=Europe/Moscow

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG_DIR="${ROOT_DIR}/tmp/cron_logs"
LOCK_FILE="/tmp/ozon_sync_all_projects.lock"
CONTAINER_NAME="wb-sync-web"
SYNC_TIMEOUT_SECONDS="${OZON_SYNC_TIMEOUT_SECONDS:-43200}"
MAX_ATTEMPTS="${OZON_SYNC_MAX_ATTEMPTS:-3}"
RETRY_SLEEP_SECONDS="${OZON_SYNC_RETRY_SLEEP_SECONDS:-900}"

mkdir -p "$LOG_DIR"

TODAY="$(date +%F)"
LOG_FILE="${LOG_DIR}/sync_ozon_projects_${TODAY}.log"

exec 9>"$LOCK_FILE"
if ! flock -n 9; then
  echo "[$(date -Is)] Another Ozon sync is already running, leaving it in background." >> "$LOG_FILE" 2>&1
  exit 0
fi

{
  echo "[$(date -Is)] Starting Ozon all-project sync"
  cd "$ROOT_DIR"
  docker compose up -d wb-sync-web

  attempt=1
  while [[ "$attempt" -le "$MAX_ATTEMPTS" ]]; do
    echo "[$(date -Is)] Ozon attempt ${attempt}/${MAX_ATTEMPTS}: python -u /app/scripts/sync_all_projects.py --only ozon $*"
    if timeout "$SYNC_TIMEOUT_SECONDS" docker exec -e TZ=Europe/Moscow "$CONTAINER_NAME" \
      python -u /app/scripts/sync_all_projects.py --only ozon "$@"; then
      echo "[$(date -Is)] Finished Ozon all-project sync"
      exit 0
    fi

    echo "[$(date -Is)] Ozon attempt ${attempt}/${MAX_ATTEMPTS} failed."
    attempt=$((attempt + 1))
    if [[ "$attempt" -le "$MAX_ATTEMPTS" ]]; then
      echo "[$(date -Is)] Sleeping ${RETRY_SLEEP_SECONDS}s before retry."
      sleep "$RETRY_SLEEP_SECONDS"
    fi
  done

  echo "[$(date -Is)] Ozon all-project sync failed after ${MAX_ATTEMPTS} attempts"
  exit 1
} >> "$LOG_FILE" 2>&1
