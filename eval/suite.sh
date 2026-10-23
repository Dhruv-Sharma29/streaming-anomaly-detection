#!/usr/bin/env bash
# Correctness suite: every run must match the reference exactly (see eval/run_pipeline.py).
set -u
cd "$(dirname "$0")/.."
mkdir -p results/suite-logs
# GNU timeout on Linux; macOS has none by default (coreutils' gtimeout if installed, else no limit).
TIMEOUT="$(command -v timeout || command -v gtimeout || true)"
limit() { if [ -n "$TIMEOUT" ]; then "$TIMEOUT" "$@"; else shift; "$@"; fi; }
n=0
run() {  # full output kept per run; one summary line printed (or the error, never silently dropped)
  n=$((n + 1)); log="results/suite-logs/$(printf %02d $n).log"
  limit 900 .venv/bin/python -m eval.run_pipeline "$@" > "$log" 2>&1
  line=$(grep -E '"(exact|run_id)"' "$log" | tr -d '\n')
  echo "${line:-FAILED: $* (see $log): $(grep -v rdkafka "$log" | tail -1)}"
}
for s in 1 2 3 4 5; do run --seed "$s" --label clean; done
run --seed 6 --crash taskmanager --at 0.2 --label tm-crash
run --seed 7 --crash taskmanager --at 0.5 --label tm-crash
run --seed 8 --crash taskmanager --at 0.8 --label tm-crash
run --seed 9 --crash taskmanager --at 0.5 --before-very-late --label tm-crash-adversarial
run --seed 10 --crash job --at 0.3 --label job-restore
run --seed 11 --crash job --at 0.7 --label job-restore
run --seed 12 --crash job --at 0.5 --before-very-late --label job-restore-adversarial
run --seed 13 --crash postgres --at 0.4 --label sink-db-restart
run --seed 14 --crash postgres --at 0.7 --label sink-db-restart
run --seed 15 --crash taskmanager --at 0.35 --label tm-crash
run --seed 16 --crash job --at 0.55 --before-very-late --label job-restore-adversarial
