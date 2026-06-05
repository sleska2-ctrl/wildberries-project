#!/usr/bin/env bash
set -euo pipefail

export TZ=Europe/Moscow

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG_DIR="${ROOT_DIR}/tmp/cron_logs"
CONTAINER_NAME="wb-sync-web"
SYNC_TIMEOUT_SECONDS="${OZON_SYNC_TIMEOUT_SECONDS:-43200}"
STAGGER_SECONDS="${SYNC_CABINET_STAGGER_SECONDS:-2}"

mkdir -p "$LOG_DIR"

TODAY="$(date +%F)"
LOG_FILE="${LOG_DIR}/sync_ozon_projects_${TODAY}.log"

{
  echo "[$(date -Is)] Starting Ozon all-project sync"
  cd "$ROOT_DIR"
  docker compose up -d wb-sync-web

  echo "[$(date -Is)] Ozon independent cabinet sync: python -u /app/scripts/sync_all_projects.py --only ozon --stagger-seconds ${STAGGER_SECONDS} $*"
  timeout "$SYNC_TIMEOUT_SECONDS" docker exec -e TZ=Europe/Moscow "$CONTAINER_NAME" \
    python -u /app/scripts/sync_all_projects.py --only ozon --stagger-seconds "$STAGGER_SECONDS" "$@"
  echo "[$(date -Is)] Finished Ozon all-project sync"
} >> "$LOG_FILE" 2>&1
