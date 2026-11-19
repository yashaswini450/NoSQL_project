#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
if ! command -v tmux >/dev/null; then
  echo 'Install tmux first: sudo apt-get install tmux' >&2
  exit 1
fi
if ! tmux has-session -t nasa-demo 2>/dev/null; then
  tmux new-session -d -s nasa-demo -n demo -c "$PWD" 'bash demo.sh'
  tmux new-window -t nasa-demo -n shell -c "$PWD" "env PS1='NASA demo \\w \\$ ' bash --noprofile --norc -i"
  tmux set-option -t nasa-demo status-style 'fg=white,bg=black'
  tmux set-option -t nasa-demo message-style 'fg=white,bg=black'
  tmux set-option -t nasa-demo window-status-current-style 'fg=white,bg=black,bold'
  tmux set-option -t nasa-demo status-left ' NASA demo '
  tmux set-option -t nasa-demo status-right ' Ctrl+B,N: shell | Ctrl+B,D: detach '
  tmux select-window -t nasa-demo:demo
fi
echo 'Ready. In Ubuntu: tmux attach -t nasa-demo'
echo 'From PowerShell: wsl -d Ubuntu-24.04 --exec tmux attach -t nasa-demo'
