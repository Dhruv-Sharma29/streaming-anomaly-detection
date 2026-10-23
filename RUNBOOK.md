# Runbook — the steps that need running services

Everything in [start.md](start.md) runs without daemons. The steps below start PostgreSQL, a
single-node Kafka (KRaft) and a Flink standalone cluster (1 JobManager + 1 TaskManager) as
user-space processes from `.tools/` (`infra/stack.py`). They are the steps that produced
`results/`. They were **not re-run** when these docs were written (2026-10-23). The latest recorded
runs are from 2026-10-06.

Prerequisites: start.md step 1 (toolchain, `uv sync`, the job jar). One host, ports on 127.0.0.1:

| Service | Port | Override |
| --- | --- | --- |
| PostgreSQL | 19432 | `STACK_PORT_BASE` (default 19000) shifts all ports |
| Kafka | 19092 (controller 19093) | |
| Flink REST / UI | 19081 | |
| Dashboard | 19080 | `scripts/demo.sh` |

State, logs, pid files and checkpoints go to `.run/` (override with `STACK_RUN_DIR`).

---

## 1. Start the stack

```bash
uv run python -m infra.stack up        # idempotent; prints {"kafka": true, ..., "postgres": true}
uv run python -m infra.stack status
```

`up` initialises the PostgreSQL data directory and the tables on first use and formats Kafka
storage. It then starts the JobManager and one TaskManager with 8 slots, a 10 s heartbeat timeout,
a fixed-delay restart strategy and checkpoints retained on cancel (`flink_conf_dir`).

## 2. One clean run, compared exactly

```bash
uv run python -m eval.run_pipeline --seed 1 --label clean
```

Each run uses a fresh topic and consumer group and truncates the tables first. It prints the record
described in start.md § 6 and appends it to `results/raw/pipeline_runs.jsonl`. Expect
`"exact": true`. For reference, seed 1 settled in 1.0 s after the input finished
(`results/suite-logs/01.log`).

## 3. One crash run

```bash
uv run python -m eval.run_pipeline --seed 2 --crash taskmanager --at 0.4    # SIGKILL TM at 40 % of input
uv run python -m eval.run_pipeline --seed 3 --crash job --at 0.5            # cancel, resume from checkpoint
uv run python -m eval.run_pipeline --seed 4 --crash postgres --at 0.5       # sink DB down 8 s
uv run python -m eval.run_pipeline --seed 9 --crash taskmanager --at 0.5 --before-very-late   # adversarial
```

Remember what these do and don't test: by default the harness pauses 8 s before the failure, so the
job is idle when it dies. Add `--inflight` to crash while the input is still arriving
([docs/03-recovery.md](docs/03-recovery.md)):

```bash
uv run python -m eval.run_pipeline --seed 22 --crash taskmanager --at 0.5 --inflight
```

## 4. The correctness suite (~15 minutes)

```bash
eval/suite.sh
```

30 runs with fixed seeds: 16 with the job idle at the failure (5 clean, 5 TaskManager kills, 4 job
restores, 2 database outages), 8 in-flight (3 TaskManager kills, 3 job restores, 2 database outages)
and 6 mid-checkpoint (3 TaskManager kills, 3 job restores). It
prints one `run_id … exact` line per run, or `FAILED: …` with the log's last line, and
**overwrites** `results/suite-logs/NN.log`. Each run is capped at 900 s by `timeout`/`gtimeout`
when available. The recorded pass ran from `20261023T114036Z` to `20261023T115319Z` plus the last run, about
13 minutes (`results/suite.log`).
Pass condition: 30 × `"exact": true`. Then update
[docs/05-evaluation.md](docs/05-evaluation.md) if any number moved.

## 5. The demo, on the dashboard

```bash
scripts/demo.sh            # then open http://127.0.0.1:19080
```

Starts the stack and the dashboard, then replays seed 42 slowly (300 records/s) with a TaskManager
SIGKILL at 50 %. Watch windows, alerts, late and dead-letter counts and the job state recover. It
ends with the comparison: `results/demo.log` is the recorded run (`"exact": true`, recovery 9.4 s).

## 6. Load and latency (needs the cluster to itself)

```bash
uv run python -m eval.load capacity --rate-per-service 1000 --duration-s 600
uv run python -m eval.load latency --speed 10 --duration-s 600 --label flush-on-checkpoint
uv run python -m eval.load latency --speed 10 --duration-s 600 --sink-flush-ms 200 --label flush-200ms
```

Results are appended to `results/raw/load_runs.jsonl`. The capacity command is the README's. The
latency arguments are reconstructed from the recorded runs (speed 10, label, flush interval and
240 windows, which implies a 600 s scenario), not from a stored command line **[verify]**.
Capacity preloads the whole backlog (about 2.5 M records at these settings) before the job starts.

## 7. Recovery timing re-measurement (optional)

The suite's TaskManager recovery times predate the timer fix ([docs/03-recovery.md](docs/03-recovery.md)
§ Recovery time). The re-measured values came from three extra runs labelled `tm-recovery-timing`,
seeds 17–19 at 50 %:

```bash
for s in 17 18 19; do uv run python -m eval.run_pipeline --seed $s --crash taskmanager --at 0.5 --label tm-recovery-timing; done
```

The seeds, crash point and label are from `results/raw/pipeline_runs.jsonl`. The exact command
line is not stored **[verify]**.

## 8. Stop

```bash
uv run python -m infra.stack down
```

Stops Flink, Kafka and PostgreSQL. `.run/` keeps data and checkpoints. Delete it for a clean slate.

---

## Not in this runbook

- **Containers** (no container runtime on the build host) and the **managed-cloud variant**. The
  cloud variant needs a spending cap, a resource inventory and a teardown script first (SPEC.md).
  Nothing in the repo provisions cloud resources.
