#!/usr/bin/env bash
set -euo pipefail

export TZ=Europe/Moscow

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG_DIR="${ROOT_DIR}/tmp/cron_logs"
CONTAINER_NAME="wb-sync-web"
SYNC_TIMEOUT_SECONDS="${WB_SYNC_TIMEOUT_SECONDS:-21600}"
STAGGER_SECONDS="${SYNC_CABINET_STAGGER_SECONDS:-2}"

mkdir -p "$LOG_DIR"

TODAY="$(date +%F)"
LOG_FILE="${LOG_DIR}/sync_all_projects_${TODAY}.log"

{
  if [[ "$#" -eq 0 ]]; then
    set -- --only wb
  fi

  echo "[$(date -Is)] Starting all-project sync: $*"
  cd "$ROOT_DIR"
  docker compose up -d wb-sync-web
  timeout "$SYNC_TIMEOUT_SECONDS" docker exec -e TZ=Europe/Moscow "$CONTAINER_NAME" \
    python -u /app/scripts/sync_all_projects.py --stagger-seconds "$STAGGER_SECONDS" "$@"
  echo "[$(date -Is)] Finished all-project sync"
} >> "$LOG_FILE" 2>&1
