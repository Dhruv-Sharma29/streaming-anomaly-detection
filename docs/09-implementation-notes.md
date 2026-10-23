# 09 — Implementation notes: where the code departs from the plan, and why

SPEC.md and PLAN.md are the plan. [02](02-semantics.md) was written first, as PLAN.md asks. This
doc records every place where building the system changed one of them, or where a later code check
found the docs saying something the code doesn't do. PLAN.md's own "Decisions and deviations" list
is the short version. The evidence for each entry is named in it.

## Corrections to the docs, found by reading the code (2026-10-23)

### 1. The Kafka key is `event_id`, not `service`

[02](02-semantics.md) said `service` is "also the Kafka key". `generator/generate.py`
(`deliveries`) keys every record by `event_id`. The reason is in its comment: keying by 4 services
could leave a partition empty and stall the watermark, and keying by id keeps a duplicate on the
same partition as its original. With one partition the choice no longer affects ordering, but the
doc was wrong. Annotated in place in 02.

### 2. Dead letters go to a PostgreSQL table, not an `events-dlq` topic

02 and PLAN.md M3 describe a dead-letter *topic*. The job writes invalid records from a side output
straight to the `dlq` table, keyed by `(kafka_partition, kafka_offset)`, with the reason and the raw
value (`StreamingJob.java`, `infra/stack.py`). That is still a visible, counted path. The
dashboard shows counts per reason, and every run compares them with the reference. It is not a
replayable topic, so a consumer can't reprocess dead letters from Kafka.

### 3. Ingestion time is not recorded by the job

02 says the Kafka record timestamp is "recorded for latency measurement only". The job never reads
it. `eval/load.py` measures latency from the producer's wall-clock send time to `first_written_at`.
The rule that matters, ingestion time never used for windowing, holds.

## Bugs the exact comparison found (each reproduced, then fixed)

### 4. Periodic watermarks lag at replay speed

Flink's default 200 ms periodic emission let the watermark trail by about 9 s of event time at
~46× replay, so late decisions depended on timing. Fix: `PerEventWatermarks` emits after every
event. Source: README, 02, the `PerEventWatermarks.java` comment. No separate run record of the
failure is in `results/`.

### 5. Partition skew changed late-drop decisions

With 3 partitions, Flink's watermark is the minimum across them. During start-up catch-up one
partition lagged, and Flink applied very-late events that the reference dropped (README: 8 events).
Record: `results/raw/development_runs.jsonl`, label `e2e-final-windows`. It shows 6 windows
mismatched, each with Flink's `request_count` one higher (e.g. 183 vs 182), and `late_match: false`.
The next run, `e2e-single-partition`, is exact. Fix: one partition and an ordered chain at
parallelism 1.

### 6. A parallel validate step reordered records

A parallel validate step "once reordered records round-robin before the lateness filter",
silently changing arrival order (comment in `StreamingJob.java`). Fix: everything up to and
including lateness is chained at parallelism 1, and `check_ordering_chain` fails any run whose job
graph differs.

### 7. Flink does not checkpoint watermarks

After a restore, the window operator's watermark starts at −∞. A very-late record replayed first
re-created a purged window, and the upsert overwrote a 197-event row with a 1-event aggregate.
Record: `adversarial-restore-no-guard` in `development_runs.jsonl`. Fix: `LatenessFilter`, with max
event time in checkpointed operator state. Flink's window operator still windows, behind a tripwire
that must stay empty.

The first run *after* this fix (`20261006T095335Z-s1-job-9751`, label `adversarial-restore-fixed`)
was still not exact: 8 windows mismatched, Flink's counts 1–2 *below* the reference, and
`late_match: false`. The next run with the same label and seed is exact. The repo doesn't record
what changed between the two **[verify]**. One plausible candidate is entry 6, which also changes
lateness decisions, but nothing in `results/` links them.

### 8. Alerts on first firings were not reproducible

The first end-to-end run (`first-e2e`) matched every window but not the alerts: a window's first
firing depends on watermark lag. Fix: `EwmaDetector` runs on finalized windows (timer at window
max timestamp + L). Cost: 30 s more emission delay ([04](04-detection.md)).

### 9. Harness: a leaked job, and a recovery timer that stopped early

A run that raised before cancelling its job left it holding task slots, which broke a later run.
`clean_slate()` now cancels leftovers and waits for free slots. The superseded suite pass
(`results/raw/superseded_suite_runs.jsonl`) has no seed 4, and a `clean-s4-debug` run follows it.
Separately, waiting for RUNNING after a TaskManager kill could return before the JobManager noticed
the loss (10 s heartbeat timeout), giving a spurious ~3.1 s. `crash()` now also waits for
`numRestarts` to rise. The current suite logs were recorded before that fix, so their TaskManager
recovery times are not used ([03](03-recovery.md) § Recovery time).

### 10. Capacity measurement v1 sampled nothing and had one timing

The first capacity run (`results/raw/load_runs_v1.jsonl`, also `results/load.log`) reports
`backpressure_samples: 0`, so its "no backpressure" was vacuous. It also gives a single drain time.
v2 (`load_runs.jsonl`) samples each vertex 6 times and reports throughput as a range. "From submit"
includes job start-up, and "from RUNNING" starts after the source has already begun consuming.

## Honest downgrades

### 11. Crash points: idle job first; in-flight and mid-checkpoint added 2026-10-23

PLAN.md M3: "kill TaskManager before sink write, after sink write, mid-checkpoint". The first 16 runs
crash at input fractions after an 8 s pause, with the job idle and fully checkpointed. Two modes
close the rest: `--inflight` (the input keeps arriving while the failure happens; 8/8 exact, sink
writes past the last checkpoint replayed) and `--mid-checkpoint` (6/6 exact, crash while a checkpoint
is in progress with its rows already written) ([03](03-recovery.md) § Idle, in-flight and mid-checkpoint crashes).

### 11a. A test-only hook in the job for mid-checkpoint crashes

Checkpoints take milliseconds every 3 s, so no in-flight run ever caught one in progress.
`UpsertSink.holdingCheckpoints(ms)`, set from the job's `--checkpoint-hold-ms` (default 0) on the
windows sink, sleeps after the pre-checkpoint flush has written its rows, holding the barrier and
so the checkpoint open. With 0 nothing changes; the jar was rebuilt and all 30 suite runs and the
Python/Java parity test pass on it. Production runs never set it.

### 11b. TaskManager failover was detected through a metric that REST didn't reliably show (fixed)

The failover detector in `crash()` waited for the job's `numRestarts` metric to rise, capped at
120 s. In the first 2026-10-23 suite it reported 3.1 s to the cap for the same failure, and 7 of 11
TaskManager runs hit the cap, although the JobManager log shows those jobs restarting at the kill
and running again on the new TaskManager about 4 s later. Two things combined. Polled through REST,
the metric was not a reliable signal: a probe showed it rising only some seconds after the restart,
and in the capped runs it never rose at all. And the early 3.1 s values were the counter rising from
an unrelated restart (§ 11c), not from the kill. The detector now reads the job's own state
timestamp: the first RESTARTING after the kill. In the final suite failover is noticed 2.5–6.7 s
after the kill in all 11 TaskManager runs ([03](03-recovery.md) § Recovery time).

### 11c. The first sink connection of a job could fail with "No suitable driver" (fixed)

The JobManager logs of 2026-10-06 and 2026-10-23 show `SQLException: No suitable driver found for
jdbc:postgresql://…` dozens of times: the job jar is loaded child-first, where `DriverManager`'s
service discovery may not see the PostgreSQL driver, so a job's first connection could fail and the
job restarted once, from the source, before doing anything. Results stayed exact (that is replay
working), but "clean" runs were not failure-free. `UpsertSink` now loads `org.postgresql.Driver`
explicitly before connecting, as Flink's own JDBC connector does. The final suite's JobManager log
has 0 such errors.

### 11d. REST views were cached for 3 s, the checkpoint interval (fixed)

Flink caches the job, checkpoint and metric views served by REST for `web.refresh-interval`, 3 s by
default, the same as the job's checkpoint interval. A poll could then lock onto one phase of the
checkpoint cycle: in one suite two `--mid-checkpoint` runs saw no checkpoint in progress for 60 s
although the JobManager log shows checkpoints taking 2 s of every 3. Cached job state also added up
to a few seconds to every recovery time the harness measured. `infra/stack.py` now sets
`web.refresh-interval: 500 ms` and `rest.cache.checkpoint-statistics.timeout: 100 ms`; this changes
what REST reports, not what the job does. Recovery times recorded before this change are not
comparable with the current ones.

### 12. "Backpressure bounded" is untested

No vertex ever reported backpressure (`max_backpressured` 0.0 everywhere in v2). The backlog run
shows no silent drop, because it is exact over 2.5 M records. It cannot show that backpressure is
*visible and bounded*, because there was none.

### 13. The fixed-budget EWMA is not the deployed EWMA

Under the 0.5-false-alerts-per-run budget, the selection chose z = 3.5. The job runs z = 4.0
(`semantics/constants.json`). The README labels that row "runs in the Flink job". The deployed
setting's quality is the M4 result (`detection.json`: held-out F1 0.966, stationary only).
[05](05-evaluation.md) reports both.

### 14. The generator was made harder to evaluate detectors

With the first effect sizes, a static threshold reached 99 % held-out recall and detectors could
not be separated. The low end of every effect was widened (comment on `EFFECTS` in
`generator/generate.py`). M5 then added the non-stationary mode, which leaves stationary scenarios
byte-identical (pinned by a SHA-256 test), so the correctness suite's inputs didn't change.

### 15. The learned detector's threshold was first chosen on its own training data

There "everything looks perfect" (`eval/detection_budget.py` docstring). The knob is now chosen on
out-of-fold dev predictions.

## Stale text in the code (not changed: this was a docs-only pass)

### 16. Comments and docstrings that no longer match

| Where | Says | Actually |
| --- | --- | --- |
| `eval/detection_budget.py` docstring | out-of-fold split "dev seeds 1–10 … 11–20" | `OutOfFold` splits by `seed % 2` |
| `generator/generate.py` docstring | "`check_margins` verifies it" | No such function; `reference.compute` raises `AmbiguousDecision`, tested over 20 seeds |
| `eval/run_pipeline.py` docstring | compares "first-firing alerts" | compares alerts from finalized windows |
| `job/.../DetectorCli.java` | input is "first-firing windows" | the tests pass final windows |
| `eval/run_pipeline.py`, `clean_slate` | evidence in "results/suite.log, seed 4" | that pass is now `results/raw/superseded_suite_runs.jsonl` |
| 9 code comments | `docs/semantics.md` | updated to `docs/02-semantics.md` (comment-only change, 2026-10-23); `PerEventWatermarks.java`'s "Determinism and replay speed" section now exists in 02 |

## Deferred, not dropped

- More than one input partition: per-partition lateness or watermark alignment with a larger
  margin, plus a new reference model ([02](02-semantics.md) § Scaling beyond one partition).
- Containers (no runtime on the host) and the optional cloud variant (needs a budget).
- The learned detector inside the job.
- Crash modes during active processing; a backpressure run ([06](06-roadmap.md)).
