#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
STATE_ROOT="${NASA_STATE_DIR:-$HOME/.local/share/nasa-phase1}"
if ! test -x "$STATE_ROOT/venv/bin/python"; then
  echo 'Runtime missing. Run: bash setup.sh' >&2
  exit 1
fi
bash services.sh start
exec bash run.sh demo.py "$@"
