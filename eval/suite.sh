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
# In-flight failures: the input keeps arriving while the failure happens, so records after the last
# completed checkpoint (some already written to the sink) must be replayed (eval/run_pipeline.py --inflight).
run --seed 21 --crash taskmanager --at 0.3 --inflight --label tm-crash-inflight
run --seed 22 --crash taskmanager --at 0.5 --inflight --label tm-crash-inflight
run --seed 23 --crash taskmanager --at 0.8 --inflight --label tm-crash-inflight
run --seed 24 --crash job --at 0.3 --inflight --label job-restore-inflight
run --seed 25 --crash job --at 0.5 --inflight --label job-restore-inflight
run --seed 26 --crash job --at 0.8 --inflight --label job-restore-inflight
run --seed 27 --crash postgres --at 0.4 --inflight --label sink-db-restart-inflight
run --seed 28 --crash postgres --at 0.7 --inflight --label sink-db-restart-inflight
# Mid-checkpoint failures: checkpoints are held open for 2 s after the sink has written its rows, and the
# failure is injected only while one is in progress (eval/run_pipeline.py --mid-checkpoint).
run --seed 31 --crash taskmanager --at 0.3 --mid-checkpoint --label tm-crash-mid-checkpoint
run --seed 32 --crash taskmanager --at 0.5 --mid-checkpoint --label tm-crash-mid-checkpoint
run --seed 33 --crash taskmanager --at 0.8 --mid-checkpoint --label tm-crash-mid-checkpoint
run --seed 34 --crash job --at 0.3 --mid-checkpoint --label job-restore-mid-checkpoint
run --seed 35 --crash job --at 0.5 --mid-checkpoint --label job-restore-mid-checkpoint
run --seed 36 --crash job --at 0.8 --mid-checkpoint --label job-restore-mid-checkpoint
