#!/usr/bin/env bash
# 停止所有演示进程
cd "$(dirname "$0")"
for f in data/*.pid; do
  [ -f "$f" ] || continue
  pid=$(cat "$f")
  if kill -0 "$pid" 2>/dev/null; then
    kill "$pid" 2>/dev/null
    echo "stopped $pid ($f)"
  fi
  rm -f "$f"
done
exit 0
