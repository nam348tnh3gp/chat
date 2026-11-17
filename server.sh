#!/bin/bash
set -e
cd "$(dirname "$0")"

[ -d .venv ] || python3 -m venv .venv
source .venv/bin/activate

SUM=$(cksum < requirements.txt)
if [ "$SUM" != "$(cat .venv/.req_sum 2>/dev/null)" ]; then
  echo "Đang cài thư viện (lần đầu có thể mất vài phút do biên dịch greenlet, đừng bấm Ctrl+C)..."
  pip install --disable-pip-version-check -r requirements.txt
  echo "$SUM" > .venv/.req_sum
fi

# Load .env WITHOUT overriding existing environment variables.
# Priority: real env > .env > defaults in code.
if [ -f .env ]; then
  while IFS= read -r line || [ -n "$line" ]; do
    line="${line%$'\r'}"
    [ -z "$line" ] && continue
    case "$line" in \#*) continue ;; esac
    line="${line#export }"
    case "$line" in *=*) ;; *) continue ;; esac
    key="${line%%=*}"
    val="${line#*=}"
    case "$val" in
      \"*\") val="${val:1:${#val}-2}" ;;
      \'*\') val="${val:1:${#val}-2}" ;;
    esac
    if [ -z "${!key+x}" ]; then
      export "$key=$val"
    fi
  done < .env
fi

# Parse các tham số dòng lệnh một cách an toàn
while [[ "$#" -gt 0 ]]; do
  case "$1" in
    --polling)
      export SOCKETIO_ASYNC_MODE="threading"
      ;;
    --eventlet|--evenlet)
      export SOCKETIO_ASYNC_MODE="eventlet"
      ;;
    *)
      # Bỏ qua các tham số không hợp lệ khác nếu có
      ;;
  esac
  shift
done

exec python3 server.py
