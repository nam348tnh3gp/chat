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

if [ -f .env ]; then
  set -a; source .env; set +a
fi

python3 server.py
