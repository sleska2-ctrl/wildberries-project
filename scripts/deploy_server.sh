#!/usr/bin/env bash
set -euo pipefail

REMOTE_HOST="${1:-KZ}"
REMOTE_BASE_DIR="${2:-/opt/wildberries}"
REMOTE_APP_DIR="${REMOTE_BASE_DIR}/app"
TMP_DIR="$(mktemp -d)"
ARCHIVE_PATH="${TMP_DIR}/wildberries-app.tgz"
STAGE_DIR="${TMP_DIR}/stage"
SECRETS_DIR="${STAGE_DIR}/secrets"
PUBLIC_PORT="${3:-18765}"

cleanup() {
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${ROOT_DIR}/.env"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "Не найден ${ENV_FILE}" >&2
  exit 1
fi

SERVICE_ACCOUNT_SOURCE="$(sed -n 's/^GOOGLE_SERVICE_ACCOUNT_FILE=//p' "$ENV_FILE" | tail -n 1)"
if [[ -z "$SERVICE_ACCOUNT_SOURCE" ]]; then
  echo "В .env не указан GOOGLE_SERVICE_ACCOUNT_FILE" >&2
  exit 1
fi

if [[ ! -f "$SERVICE_ACCOUNT_SOURCE" ]]; then
  echo "Не найден файл сервисного аккаунта: ${SERVICE_ACCOUNT_SOURCE}" >&2
  exit 1
fi

mkdir -p "$STAGE_DIR"
cp -R \
  "${ROOT_DIR}/src" \
  "${ROOT_DIR}/scripts" \
  "${ROOT_DIR}/Dockerfile" \
  "${ROOT_DIR}/docker-compose.yml" \
  "${ROOT_DIR}/requirements.txt" \
  "${ROOT_DIR}/run_sync.py" \
  "${ROOT_DIR}/web_app.py" \
  "$STAGE_DIR"

mkdir -p "$SECRETS_DIR"
cp "$SERVICE_ACCOUNT_SOURCE" "${SECRETS_DIR}/google-service-account.json"

python3 - <<'PY' "$ENV_FILE" "${STAGE_DIR}/.env" "$PUBLIC_PORT"
from pathlib import Path
import sys

source_env = Path(sys.argv[1]).read_text().splitlines()
target_env = Path(sys.argv[2])
public_port = sys.argv[3]

rewritten = []
has_service_account = False
has_public_port = False
for line in source_env:
    if line.startswith("GOOGLE_SERVICE_ACCOUNT_FILE="):
        rewritten.append("GOOGLE_SERVICE_ACCOUNT_FILE=/run/secrets/google-service-account.json")
        has_service_account = True
    elif line.startswith("WEB_PUBLIC_PORT="):
        rewritten.append(f"WEB_PUBLIC_PORT={public_port}")
        has_public_port = True
    else:
        rewritten.append(line)

if not has_service_account:
    rewritten.append("GOOGLE_SERVICE_ACCOUNT_FILE=/run/secrets/google-service-account.json")
if not has_public_port:
    rewritten.append(f"WEB_PUBLIC_PORT={public_port}")

target_env.write_text("\n".join(rewritten) + "\n")
PY

COPYFILE_DISABLE=1 tar -czf "$ARCHIVE_PATH" -C "$STAGE_DIR" .

scp "$ARCHIVE_PATH" "${REMOTE_HOST}:/tmp/wildberries-app.tgz"
ssh "$REMOTE_HOST" "mkdir -p '${REMOTE_APP_DIR}' && tar -xzf /tmp/wildberries-app.tgz -C '${REMOTE_APP_DIR}' && rm -f /tmp/wildberries-app.tgz && cd '${REMOTE_APP_DIR}' && docker compose up -d --build"

REMOTE_IP="$(ssh "$REMOTE_HOST" "hostname -I | awk '{print \$1}'")"
echo "Деплой завершен: http://${REMOTE_IP}:${PUBLIC_PORT}"
