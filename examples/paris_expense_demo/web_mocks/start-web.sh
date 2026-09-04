#!/usr/bin/env bash
# Start static order pages for paris_expense_demo.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"
PORT="${1:-8765}"

if lsof -nP -iTCP:"$PORT" -sTCP:LISTEN >/dev/null 2>&1; then
  echo "Port $PORT is already in use."
  echo "Open: http://127.0.0.1:$PORT/"
  echo "To restart: lsof -tiTCP:$PORT -sTCP:LISTEN | xargs kill; then re-run this script."
  exit 0
fi

cd "$ROOT"
echo "Serving $ROOT on http://127.0.0.1:$PORT/"
echo "Keep this terminal open. Press Ctrl+C to stop."
exec python3 -m http.server "$PORT" --bind 127.0.0.1
