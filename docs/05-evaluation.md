# 05 — Evaluation

Two results, reported separately on purpose: **(1) pipeline correctness under failure** and
**(2) detector quality**, plus a short load section. Every number below is copied from a file in
`results/`, and the file is named next to it. All runs are from 2026-10-06 on one host (the GX10,
per README), as local processes.

## What question each part answers

| Part | Question | Pass condition |
| --- | --- | --- |
| Correctness | Does the job produce the reference's output through duplicates, disorder, lateness, malformed input and failures? | Every run exact: 0 missing / extra / mismatched windows, same late-dropped id set, same DLQ counts per reason, identical alerts, empty tripwire |
| Detection | How well does each detector find labelled anomalies at a fixed false-alert budget, on runs it was not tuned on? | No pass/fail; precision, recall, delay, false alerts per run, with intervals |
| Load | How fast does the ordered chain drain a backlog, and how stale are results? | Output still exact under load |

## Correctness under failure: 30/30 exact

`eval/suite.sh` has three groups: 16 runs with the job idle at the failure, 8 with the input still
arriving (`--inflight`) and 6 inside a checkpoint (`--mid-checkpoint`). All 30 were re-run on
2026-10-23 on the final job jar (JDBC driver loaded explicitly, mid-checkpoint hook off unless asked
for) and harness (failover read from job state timestamps, REST views refreshed every 500 ms); the
suite took 13 minutes. Earlier passes are in git history.

### Job idle at the failure: 16/16

`eval/suite.sh` → `results/suite.log` (one line per run) and `results/suite-logs/NN.log` (full
record). Each scenario is 10 minutes of event time for 4 services, produced at 4,000 records/s,
about 46× real time.

| Log | Seed | Label | Cut (% of input) | Records | Window writes | Windows | Alerts | Recovery (s) | Exact |
|---|---|---|---|---|---|---|---|---|---|
| 01 | 1 | clean |  | 51,584 | 625 | 240/240 | 5/5 | — | yes |
| 02 | 2 | clean |  | 51,207 | 619 | 240/240 | 5/5 | — | yes |
| 03 | 3 | clean |  | 50,808 | 617 | 240/240 | 4/4 | — | yes |
| 04 | 4 | clean |  | 51,659 | 639 | 240/240 | 4/4 | — | yes |
| 05 | 5 | clean |  | 50,893 | 644 | 240/240 | 3/3 | — | yes |
| 06 | 6 | tm-crash | 20 | 52,028 | 628 | 240/240 | 4/4 | 7.8 (2.5) | yes |
| 07 | 7 | tm-crash | 50 | 51,043 | 632 | 240/240 | 4/4 | 7.8 (2.9) | yes |
| 08 | 8 | tm-crash | 80 | 51,035 | 644 | 240/240 | 5/5 | 7.8 (3.0) | yes |
| 09 | 9 | tm-crash-adversarial | 50 → next very-late | 51,630 | 659 | 240/240 | 5/5 | 12.1 (2.7) | yes |
| 10 | 10 | job-restore | 30 | 50,826 | 637 | 240/240 | 5/5 | 3.7 | yes |
| 11 | 11 | job-restore | 70 | 50,560 | 621 | 240/240 | 5/5 | 3.5 | yes |
| 12 | 12 | job-restore-adversarial | 50 → next very-late | 51,728 | 621 | 240/240 | 4/4 | 3.6 | yes |
| 13 | 13 | sink-db-restart | 40 | 52,449 | 590 | 240/240 | 4/4 | 8.5 | yes |
| 14 | 14 | sink-db-restart | 70 | 50,762 | 586 | 240/240 | 4/4 | 8.5 | yes |
| 15 | 15 | tm-crash | 35 | 51,024 | 622 | 240/240 | 4/4 | 9.9 (2.8) | yes |
| 16 | 16 | job-restore-adversarial | 55 → next very-late | 52,135 | 639 | 240/240 | 5/5 | 3.6 | yes |

Recovery is in seconds; for a TaskManager kill, the bracket is when the failover was noticed (the job
entered RESTARTING). Recovery times from before 2026-10-23 were measured through a 3 s REST cache
and are not comparable ([03](03-recovery.md) § Recovery time).

Every run also has `late_match`, `dlq_match` and `alerts_match` true and `window_operator_late`
0. *Window writes* exceeds 240 because late events and repeated flushes upsert a row again. It is
not by itself a measure of replay.

**What one seed contains.** Seed 1, from the reference (`python -m reference.compute`, re-run
2026-10-23): 51,584 records, 50,383 accepted, 994 duplicates dropped, 116 late-dropped, 90 dead
letters (42 malformed JSON, 48 unsupported schema version), 1 flush marker, 240 windows. The
generator's rates per valid event are 2 % duplicates, 1 % late (25–32 s), 0.3 % very late
(75–110 s), 0.1 % malformed and 0.1 % schema v3, with no late classes in the last 130 s
(`generator/generate.py`).

**Superseded suite.** `results/raw/superseded_suite_runs.jsonl` holds an earlier full pass (all
exact). Seed 4 is missing from it, and a separate `clean-s4-debug` run follows: that pass hit the
leaked-job harness bug ([09](09-implementation-notes.md) § 9). The suite above is the re-run.

### Input still arriving: 8/8

`--inflight` injects the failure while the input is still being produced ([03](03-recovery.md)
§ Idle, in-flight and mid-checkpoint crashes). `results/suite-logs/17–24.log`.

| Log | Seed | Failure | Cut | Checkpoint in progress | Producer running | Last checkpoint age | Sink writes before crash | Recovery (s) | Exact |
|---|---|---|---|---|---|---|---|---|---|
| 17 | 21 | TaskManager kill | 30% | 0 | yes | 2.9 s | 36 | 8.3 (3.5) | yes |
| 18 | 22 | TaskManager kill | 50% | 0 | yes | 1.85 s | 222 | 9.4 (6.7) | yes |
| 19 | 23 | TaskManager kill | 80% | 0 | yes | 2.78 s | 364 | 8.3 (3.1) | yes |
| 20 | 24 | job cancel + restore | 30% | 0 | yes | 0.98 s | 141 | 3.6 | yes |
| 21 | 25 | job cancel + restore | 50% | 0 | yes | 1.36 s | 268 | 5.7 | yes |
| 22 | 26 | job cancel + restore | 80% | 0 | yes | 0.29 s | 566 | 2.5 | yes |
| 23 | 27 | sink database down 8 s | 40% | 0 | yes | 1.73 s | 175 | 10.1 | yes |
| 24 | 28 | sink database down 8 s | 70% | 0 | yes | 1.54 s | 407 | 9.9 | yes |

### Inside a checkpoint: 6/6

`--mid-checkpoint` holds each checkpoint open for 2 s after the sink has written its rows and crashes
only while Flink reports one in progress. `results/suite-logs/25–30.log`.

| Log | Seed | Failure | Cut | Checkpoint in progress | Producer running | Last checkpoint age | Sink writes before crash | Recovery (s) | Exact |
|---|---|---|---|---|---|---|---|---|---|
| 25 | 31 | TaskManager kill | 30% | 1 | yes | 1.12 s | 210 | 7.3 (4.5) | yes |
| 26 | 32 | TaskManager kill | 50% | 1 | yes | 1.1 s | 382 | 8.3 (3.7) | yes |
| 27 | 33 | TaskManager kill | 80% | 1 | yes | 2.67 s | 449 | 7.8 (2.9) | yes |
| 28 | 34 | job cancel + restore | 30% | 1 | yes | 1.03 s | 217 | 3.1 | yes |
| 29 | 35 | job cancel + restore | 50% | 1 | yes | 1.12 s | 340 | 3.1 | yes |
| 30 | 36 | job cancel + restore | 80% | 1 | yes | 1.09 s | 567 | 3.1 | yes |

In all 6 a checkpoint was in progress at the failure with its rows already in the database, and the
producer was still sending.

**Scope of the claim.** Final table contents are exact after idle, in-flight and mid-checkpoint
process failures. Not claimed: end-to-end exactly-once side effects, survival of Kafka-log or disk
loss, or Kafka broker or JobManager failure.

## Detection quality

### M4: the deployed EWMA vs a static rule (stationary traffic)

`python -m eval.detection --out results/summary/detection` → `results/summary/detection.json`.
EWMA runs with the job's constants (z = 4.0), not tuned. Static (a, b, c) = (1.5, 0.02, 1.5) is
chosen on dev by best F1. Held-out = 30 runs, 150 anomalies.

| Detector | Precision | Recall | F1 | False alerts (total) | Median / p90 delay |
| --- | --- | --- | --- | --- | --- |
| EWMA z = 4.0 (as deployed) | 1.000 | 0.933 | 0.966 | 0 | 20 s / 30 s |
| Static, F1-tuned | 0.993 | 0.967 | 0.980 | 1 | 20 s / 30 s |

Read this plainly: **on stationary traffic the tuned static rule beat the untuned EWMA on recall**
(145 vs 140 of 150 detected). Recall misses concentrate in low severity. EWMA found 14/23 anomalies
with severity < 0.2 and 32/33 at 0.2–0.4. By kind, the weakest are `latency_drift` (25/30) and
`error_burst` (27/30). That result is why M5 compares detectors at a fixed false-alert budget and
adds traffic where a static baseline should fail.

### M5: three detectors, one false-alert budget, two traffic regimes

`python -m eval.detection_budget` → `results/summary/detection_budget.json`. Budget ≤ 0.5 false
alerts per run on dev (seeds 1–20, both regimes pooled). Chosen knobs: EWMA z = 3.5, static
(2.0, 0.02, 1.5), learned threshold 0.4. Held-out: seeds 101–130 in each regime.

| Detector | Stationary P / R / F1 | FA / run | Non-stationary P / R / F1 | FA / run (in benign) |
| --- | --- | --- | --- | --- |
| EWMA (z re-chosen: 3.5) | 1.000 / 0.947 / 0.973 | 0.00 | 0.937 / 0.893 / 0.915 | 0.30 (0.27) |
| Static | 0.993 / 0.927 / 0.959 | 0.03 | 0.902 / 0.920 / 0.911 | 0.50 (0.47) |
| Learned | 1.000 / 0.953 / 0.976 | 0.00 | 0.965 / 0.920 / 0.942 | 0.17 (0.17) |

Learned minus baseline, 95 % bootstrap intervals over held-out scenarios
(`learned_minus_baselines`):

| Regime | vs | Recall difference | False alerts / run difference |
| --- | --- | --- | --- |
| Stationary | EWMA | [−0.020, +0.033] | [0.000, 0.000] |
| Stationary | Static | **[+0.007, +0.053]** | [−0.100, 0.000] |
| Non-stationary | EWMA | [−0.007, +0.067] | **[−0.267, −0.033]** |
| Non-stationary | Static | [−0.033, +0.040] | **[−0.533, −0.167]** |

**What this supports.** On non-stationary traffic the learned detector raises significantly fewer
false alerts than either baseline, and its recall is not significantly different. On stationary
traffic it is indistinguishable from EWMA and has better recall than the static rule. Most EWMA
false alerts land during benign shifts (0.27 of 0.30 per run): it freezes its baseline while a
window looks anomalous, so a permanent shift looks like an anomaly that never ends. Median decision
delay is 20 s for every detector in every regime. Alerts are *emitted* 30 s later because detection
runs on finalized windows (`emission_extra_delay_s` in `detection.json`).

**What it does not support.** A claim about real traffic: the data is synthetic, the 150 held-out
anomalies per regime come from one generator, and the "benign" non-stationarity is the kind the
generator was written to produce. Low-severity recall stays weak for all three (`recall_low_severity`
0.52–0.70).

## Throughput and latency

`eval/load.py`, both modes with the exact comparison on. Raw records:
`results/raw/load_runs.jsonl` (current) and `results/raw/load_runs_v1.jsonl` = `results/load.log`
(first version). The run records don't store their arguments. The capacity parameters below come
from the README command (v2) and the `eval/load.py` defaults (v1), and the record counts are
consistent with them (4 services × rate × duration, plus faults and benign traffic shifts).

| Run | Records | Result | Exact |
| --- | --- | --- | --- |
| Capacity v2: 1000 events/s/service × 600 s, preloaded backlog | 2,547,576 | drained in 12.3 s (from RUNNING) – 14.3 s (from submit) → **178k–208k records/s** | yes |
| Capacity v1: 500 × 300 s | 635,369 | 4.1 s → 154k records/s; **0 backpressure samples** collected | yes |
| Latency (v1 file), 10× replay, flush only on full batch / before checkpoint | 240 windows | p50 1.45 s, p95 2.47 s, max 3.43 s | yes |
| Latency (v1 file), 10× replay, also flush every 200 ms | 240 windows | p50 0.29 s, p95 1.04 s, max 1.79 s | yes |

Capacity v2 sampled every vertex 6 times and saw `max_backpressured` 0.0 everywhere. That was a
measurement artifact, not an absence of backpressure: 6 samples in a 12 s drain, read through REST
views cached for 3 s, and a busy ratio Flink sometimes returns as a string. With the sampler fixed
(0.5 s interval, 500 ms REST refresh, ratios parsed as numbers), the same backlog shows backpressure,
and a deliberately slow sink shows a lot of it:

| Run (2026-10-23, 2,547,576 records preloaded) | Drain | Max backpressure: source / dedup | Exact |
| --- | --- | --- | --- |
| Normal sink | 14.1 s from submit → 181k–226k records/s | 0.28 / 0.41 | yes |
| Windows sink stalled 2.5 s at every checkpoint | 50.2 s from submit → 51k–53k records/s | **0.95 / 0.98** | yes |

The windows task itself reads as busy (1.0), not backpressured, because the sink is chained into it;
the waiting shows up upstream, at dedup and the source. Bounded: Kafka is a pull source, so the
backlog stays in Kafka and the job reads it as fast as the sink allows. Latency is
measured from the producer's send time of the record that makes a window closable to the row's
first write in PostgreSQL. It is dominated by sink flushing, which is tied to the 3 s checkpoint
interval unless the 200 ms flush is enabled. Correctness doesn't change, because the flush before
each checkpoint still happens (`UpsertSink`).

## Not measured

- Recovery or correctness with more than one input partition, or on more than one host.
- Backpressure from a slow downstream other than the sink stall used here (e.g. a slow
  PostgreSQL under a real write load).
- The learned detector inside the job, or any detector on non-stationary traffic through the
  cluster.
- Any cloud or container deployment.
