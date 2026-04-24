#!/usr/bin/env bash
set -euo pipefail

PORTS="8088,8089,8090"
FORCE="false"

usage() {
  cat <<'EOF'
Usage:
  scripts/stop_three_instances.sh [--ports 8088,8089,8090] [--force]

Options:
  --ports   Comma-separated ports to stop (default: 8088,8089,8090)
  --force   Use SIGKILL if process does not exit after SIGTERM
  -h, --help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --ports)
      PORTS="${2:-}"
      shift 2
      ;;
    --force)
      FORCE="true"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage
      exit 1
      ;;
  esac
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
LOGS_DIR="$REPO_ROOT/logs"

split_ports() {
  local input="$1"
  local IFS=','
  read -r -a arr <<< "$input"
  for p in "${arr[@]}"; do
    p="$(echo "$p" | xargs)"
    if [[ -z "$p" || ! "$p" =~ ^[0-9]+$ ]]; then
      echo "Invalid port: $p" >&2
      exit 1
    fi
    if (( p < 1 || p > 65535 )); then
      echo "Port out of range: $p" >&2
      exit 1
    fi
    echo "$p"
  done
}

process_exists() {
  local pid="$1"
  kill -0 "$pid" 2>/dev/null
}

stop_pid() {
  local pid="$1"
  local label="$2"

  if ! process_exists "$pid"; then
    echo "[skip] $label pid=$pid not running"
    return 0
  fi

  kill "$pid" 2>/dev/null || true

  for _ in {1..20}; do
    if ! process_exists "$pid"; then
      echo "[ok] stopped $label pid=$pid"
      return 0
    fi
    sleep 0.2
  done

  if [[ "$FORCE" == "true" ]]; then
    kill -9 "$pid" 2>/dev/null || true
    sleep 0.2
    if process_exists "$pid"; then
      echo "[warn] failed to force kill $label pid=$pid"
      return 1
    fi
    echo "[ok] force killed $label pid=$pid"
    return 0
  fi

  echo "[warn] still running $label pid=$pid (rerun with --force)"
  return 1
}

stopped_any="false"

for port in $(split_ports "$PORTS"); do
  pid_file="$LOGS_DIR/demo-$port.pid"

  if [[ -f "$pid_file" ]]; then
    pid="$(cat "$pid_file" | xargs || true)"
    if [[ "$pid" =~ ^[0-9]+$ ]]; then
      if stop_pid "$pid" "from pidfile port=$port"; then
        stopped_any="true"
      fi
    else
      echo "[warn] invalid pid file: $pid_file"
    fi
    rm -f "$pid_file"
  fi

  if command -v lsof >/dev/null 2>&1; then
    port_pids="$(lsof -ti tcp:"$port" -sTCP:LISTEN || true)"
  else
    port_pids=""
  fi

  if [[ -n "$port_pids" ]]; then
    while IFS= read -r p; do
      [[ -z "$p" ]] && continue
      if stop_pid "$p" "by port=$port"; then
        stopped_any="true"
      fi
    done <<< "$port_pids"
  fi

done

if [[ "$stopped_any" == "true" ]]; then
  echo "Done."
else
  echo "No running instance found for ports: $PORTS"
fi
