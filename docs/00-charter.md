# 00 — Charter

## The problem

A streaming job that computes per-service metrics is easy to make *look* right. Feed it a clean,
in-order stream and the dashboard fills up. The hard part shows up when delivery is messy and
workers die: duplicates double-count, late events either vanish or resurrect windows that were
already final, and a restart from a checkpoint quietly produces slightly different numbers. Nobody
notices, because there is nothing to compare against.

This project treats **correctness under failure** as the main question and anomaly detection as a
separate, separately reported component. Concretely: an offline reference computes the expected
output from the same input, and every run of the real pipeline must match it **exactly**: every
window row, every late-dropped event id, every dead-letter count, every alert down to its
floating-point score.

## What it does

**Input:** a seeded scenario: about 51k records for 4 services over 10 minutes of event time, with
ground-truth anomaly labels and injected delivery faults (`generator/generate.py`).

**Output:**
1. PostgreSQL tables `windows`, `alerts`, `late_events`, `dlq`, filled by a Kafka → Flink job.
2. An exact comparison with the reference (`eval/run_pipeline.py`), one JSON record per run in
   `results/raw/pipeline_runs.jsonl` and `results/suite-logs/NN.log`.
3. Detector quality on held-out scenarios, reported apart from correctness
   (`results/summary/detection.json`, `results/summary/detection_budget.json`).
4. A live dashboard over the tables and the Flink job state (`dashboard/app.py`).

## Scope

### In
- One Kafka topic with **one partition**, one Flink job, PostgreSQL as the sink, all on one host as
  local processes (`infra/stack.py`).
- Event-time tumbling windows (10 s) with bounded out-of-orderness, allowed lateness, a dedup
  horizon and a dead-letter path, all defined in [02](02-semantics.md).
- Failure injection: TaskManager SIGKILL, job cancel and restore from checkpoint, sink database
  outage, and adversarial restores ([03](03-recovery.md)).
- One in-job detector (EWMA z-score) plus two offline comparators: static thresholds and a
  gradient-boosted classifier ([04](04-detection.md)).
- Local load and latency measurement ([05](05-evaluation.md) § Throughput and latency).

### Out (deliberately, not from running out of time)
- **More than one input partition.** With several partitions, Flink's watermark is a minimum across
  partitions, and start-up skew changed late-drop decisions in a reproduced run
  ([09](09-implementation-notes.md) § 5). A single global arrival order is also what the reference
  is defined over. Scaling out needs a different semantics, sketched in [02](02-semantics.md) §
  *Scaling beyond one partition*, not a bigger cluster.
- **End-to-end exactly-once side effects.** The claim is about final table contents. Transactional
  sinks would add a commit protocol whose correctness this project can't test on one host, for a
  guarantee the deterministic-upsert design already gives for final state ([03](03-recovery.md)).
- **Containers and a managed-cloud variant.** The host has no container runtime (PLAN.md
  "Decisions and deviations"); local processes show the same failure modes (process kill). A cloud
  run needs a spending cap and a teardown script and would not inherit the local guarantees
  automatically (SPEC.md).
- **Running the learned detector inside the job.** It's evaluated offline on windows the job is
  shown to reproduce exactly; porting it to Java is engineering with no new correctness question.

### Why this scoping holds up in an interview
Every narrowing serves the exact comparison. One partition and per-event watermarks make every
decision a function of the input sequence, which is what lets a few hundred lines of Python act as
an oracle for a distributed job. Widen the scope and the comparison degrades to "close enough",
and "close enough" is where the bugs in [09](09-implementation-notes.md) were hiding. The ceiling
this imposes, one consumer thread, is measured, not hidden: 178k–208k records/s draining a backlog
([05](05-evaluation.md)).

## Non-goals

- A production anomaly detector. The detectors exist so detection quality and delay can be
  measured on labelled data, under a fixed false-alert budget.
- Throughput records. The single-threaded ordered chain is the deliberate bottleneck.
- Surviving loss of the Kafka log, the checkpoint directory or the host. One host shows process
  failure only ([02](02-semantics.md) § Not claimed).

## Success criteria

The SPEC's "important tests" and definition of done, mapped to what checks each one.

| # | Criterion | How it's checked | State |
|---|---|---|---|
| S1 | After every crash scenario, final sink output equals the reference | `eval/suite.sh`, 30 runs (16 idle, 8 in-flight, 6 mid-checkpoint), all `"exact": true` (`results/suite.log`, 2026-10-23) | Met |
| S2 | Replayed event ids don't inflate counts inside the dedup horizon | Dedup before windowing; `windows_mismatched == 0` in every run; `tests/test_reference.py` reconciliation over 20 seeds | Met |
| S3 | Late events follow the documented update/drop rule | `late_match` in every run; `test_late_event_within_lateness_is_applied`, boundary tests | Met |
| S4 | Malformed or unsupported-schema records go to a visible dead-letter path | `dlq` table + dashboard; `dlq_match` per reason in every run | Met (a table, not a topic: [09](09-implementation-notes.md) § 2) |
| S5 | Repeated anomaly windows don't create alert storms | Open after 2 / close after 3, `alert_id` upsert; `alerts_match` in every run | Met |
| S6 | A worker crash before / after sink writes, or mid-checkpoint, still matches | 8 in-flight runs crash with sink writes past the last checkpoint, and 6 crash while a checkpoint is in progress with its rows written; all exact ([03](03-recovery.md)) | Met |
| S7 | Backpressure stays visible and bounded, no silent drop | Capacity run exact, sampler on; 0.0 backpressure observed on every vertex (`results/raw/load_runs.jsonl`) | **Partly met**: no drops, but never exercised under backpressure |
| S8 | Detector quality reported separately: held-out seeds, precision/recall/delay, same alert budget | `results/summary/detection_budget.json`, 30 held-out runs per regime | Met |
| S9 | Demo: anomaly + duplicates + delays, kill a worker, recover, verify | `scripts/demo.sh` → `results/demo.log` (`"exact": true`) | Met |

## The honest risk to the thesis

An exact match proves less than it seems if the two sides could be wrong the same way. The
reference and the job share one constants file and one written semantics, so a misreading of how
Flink behaves would show up in both and pass ([07](07-risks.md) § R1). The first 16 crash points
were the easy ones, with the job idle when it died; 8 in-flight and 6 mid-checkpoint runs now cover
replay with sink overwrites ([07](07-risks.md) § R2).
