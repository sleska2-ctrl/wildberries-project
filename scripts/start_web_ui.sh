#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PID_FILE="$ROOT_DIR/.wb_web_app.pid"
LOG_FILE="$ROOT_DIR/.wb_web_app.log"
PORT="8765"
URL="http://127.0.0.1:${PORT}"

is_running() {
  local pid
  if [[ -f "$PID_FILE" ]]; then
    pid="$(cat "$PID_FILE" 2>/dev/null || true)"
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
      return 0
    fi
  fi
  return 1
}

is_port_busy() {
  lsof -tiTCP:"$PORT" -sTCP:LISTEN >/dev/null 2>&1
}

if is_running; then
  :
elif is_port_busy; then
  # Another process is already serving this port.
  server_pid="$(lsof -tiTCP:"$PORT" -sTCP:LISTEN | head -n 1 || true)"
  if [[ -n "$server_pid" ]]; then
    echo "$server_pid" > "$PID_FILE"
  fi
else
  (
    cd "$ROOT_DIR"
    source .venv/bin/activate
    PYTHONPATH=src nohup python web_app.py > "$LOG_FILE" 2>&1 &
    echo $! > "$PID_FILE"
  )

  # Wait briefly until server responds.
  for _ in {1..30}; do
    if curl -fsS "$URL" >/dev/null 2>&1; then
      break
    fi
    sleep 0.2
  done
fi

open "$URL"
echo "WB web UI: $URL"
