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

## Correctness under failure: 16/16 exact

`eval/suite.sh` → `results/suite.log` (one line per run) and `results/suite-logs/NN.log` (full
record). Each scenario is 10 minutes of event time for 4 services, produced at 4,000 records/s,
about 46× real time.

| Log | Seed | Label | Cut (% of input) | Records | Window writes | Windows | Alerts | Recovery (s) | Exact |
|---|---|---|---|---|---|---|---|---|---|
| 01 | 1 | clean |  | 51,584 | 625 | 240/240 | 5/5 | — | yes |
| 02 | 2 | clean |  | 51,207 | 618 | 240/240 | 5/5 | — | yes |
| 03 | 3 | clean |  | 50,808 | 617 | 240/240 | 4/4 | — | yes |
| 04 | 4 | clean |  | 51,659 | 635 | 240/240 | 4/4 | — | yes |
| 05 | 5 | clean |  | 50,893 | 633 | 240/240 | 3/3 | — | yes |
| 06 | 6 | tm-crash | 20 | 52,028 | 618 | 240/240 | 4/4 | 3.1 † | yes |
| 07 | 7 | tm-crash | 50 | 51,043 | 632 | 240/240 | 4/4 | 10.0 † | yes |
| 08 | 8 | tm-crash | 80 | 51,035 | 635 | 240/240 | 5/5 | 9.4 † | yes |
| 09 | 9 | tm-crash-adversarial | 50 → next very-late | 51,630 | 652 | 240/240 | 5/5 | 9.9 † | yes |
| 10 | 10 | job-restore | 30 | 50,826 | 621 | 240/240 | 5/5 | 8.5 | yes |
| 11 | 11 | job-restore | 70 | 50,560 | 608 | 240/240 | 5/5 | 8.5 | yes |
| 12 | 12 | job-restore-adversarial | 50 → next very-late | 51,728 | 564 | 240/240 | 4/4 | 8.3 | yes |
| 13 | 13 | sink-db-restart | 40 | 52,449 | 585 | 240/240 | 4/4 | 8.4 | yes |
| 14 | 14 | sink-db-restart | 70 | 50,762 | 580 | 240/240 | 4/4 | 8.4 | yes |
| 15 | 15 | tm-crash | 35 | 51,024 | 610 | 240/240 | 4/4 | 3.1 † | yes |
| 16 | 16 | job-restore-adversarial | 55 → next very-late | 52,135 | 591 | 240/240 | 5/5 | 8.3 | yes |

† Recorded with the earlier recovery timer; see [03](03-recovery.md) § Recovery time for the
re-measured values (3.2–10.0 s).

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

**Scope of the claim.** Final table contents are exact. Not claimed: end-to-end exactly-once side
effects, survival of Kafka-log or disk loss, or correctness of crashes during active processing
(failures are injected with the job idle; [03](03-recovery.md) § What the suite does not exercise).

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

Capacity v2 sampled every vertex 6 times. `max_backpressured` is 0.0 and `max_busy` 1.0 on all of
them. The single ordered chain is saturated and nothing upstream of it is waiting. Latency is
measured from the producer's send time of the record that makes a window closable to the row's
first write in PostgreSQL. It is dominated by sink flushing, which is tied to the 3 s checkpoint
interval unless the 200 ms flush is enabled. Correctness doesn't change, because the flush before
each checkpoint still happens (`UpsertSink`).

## Not measured

- Recovery or correctness with more than one input partition, or on more than one host.
- Behaviour under real backpressure (none occurred).
- Crashes during active processing ([03](03-recovery.md)).
- The learned detector inside the job, or any detector on non-stationary traffic through the
  cluster.
- Any cloud or container deployment.
