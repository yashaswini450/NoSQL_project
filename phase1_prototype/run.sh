#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
export NASA_STATE_DIR="${NASA_STATE_DIR:-$HOME/.local/share/nasa-phase1}"
export JAVA_HOME="$NASA_STATE_DIR/java"
export HADOOP_HOME="$NASA_STATE_DIR/hadoop"
export PATH="$JAVA_HOME/bin:$HADOOP_HOME/bin:$PATH"
export PYTHONPATH="$PWD${PYTHONPATH:+:$PYTHONPATH}"
exec "$NASA_STATE_DIR/venv/bin/python" "$@"
