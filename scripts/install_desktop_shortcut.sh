#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DESKTOP_DIR="$HOME/Desktop"
START_SHORTCUT="$DESKTOP_DIR/WB Sync.command"
STOP_SHORTCUT="$DESKTOP_DIR/WB Sync Stop.command"

cat > "$START_SHORTCUT" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
cd "__ROOT_DIR__"
bash scripts/start_web_ui.sh
EOF

cat > "$STOP_SHORTCUT" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
cd "__ROOT_DIR__"
bash scripts/stop_web_ui.sh
EOF

sed -i '' "s|__ROOT_DIR__|$ROOT_DIR|g" "$START_SHORTCUT" "$STOP_SHORTCUT"

chmod +x "$START_SHORTCUT" "$STOP_SHORTCUT"

echo "Created: $START_SHORTCUT"
echo "Created: $STOP_SHORTCUT"
