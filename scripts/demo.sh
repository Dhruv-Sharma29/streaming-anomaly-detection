#!/usr/bin/env bash
# Definition-of-done demo: anomalies + duplicates + delayed events are replayed slowly enough to
# watch on the dashboard, a TaskManager is SIGKILLed mid-stream, processing recovers from the last
# checkpoint, and the final output is compared with the reference (expect "exact": true).
#   scripts/demo.sh            then open http://127.0.0.1:19080
set -euo pipefail
cd "$(dirname "$0")/.."
.venv/bin/python -m infra.stack up >/dev/null
.venv/bin/python -m uvicorn dashboard.app:app --host 127.0.0.1 --port 19080 --log-level warning &
DASH=$!
trap 'kill $DASH 2>/dev/null' EXIT
echo "dashboard: http://127.0.0.1:19080"
.venv/bin/python -m eval.run_pipeline --seed 42 --crash taskmanager --at 0.5 --rate 300 --label demo 2>&1 | grep -v rdkafka
