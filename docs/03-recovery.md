# 03 — Recovery: checkpoints, the upsert sink, and the crash harness

[02](02-semantics.md) states the guarantee. This doc gives the argument for it, how the harness
attacks it, and what the harness does not attack yet.

## The claim, and the chain it rests on

> After any crash and recovery of the processing job, the final contents of the `windows`,
> `alerts`, `late_events` and `dlq` tables equal the reference.

(02 phrases the claim for `windows`; the comparison in `eval/run_pipeline.py` checks all four.)

```text
 checkpoint n completes            failure                   restore from n
 ─────────────┬───────────────────────X─────────────────────────┬──────────────────────────►
   barrier: every sink flushes        │                          │ offsets, lateness max-ts, dedup ids,
   its buffered upserts BEFORE        │ writes after n may or    │ windows, detector state: all as of n
   the checkpoint can complete        │ may not have reached PG  │ replay input after n → same values
                                                                   → upserts overwrite with same values
```

1. **Input is replayable.** Kafka retains the topic; Flink commits offsets only inside checkpoints.
   The producer is idempotent with `acks=all` (`generator/produce.py`), so the only duplicates on
   the broker are the ones the scenario injects on purpose.
2. **State and offsets are restored together** from the same checkpoint, including the lateness
   filter's max event time, which Flink itself would not restore ([02](02-semantics.md) § Who
   decides lateness).
3. **Sink writes are idempotent upserts of deterministic values.** `UpsertSink` buffers statements
   and executes them in `SinkWriter.flush`, which Flink calls before each checkpoint barrier
   (`job/.../sink/UpsertSink.java`). Anything before a completed checkpoint is therefore in
   PostgreSQL. Anything after it may be written again on replay, and gets the same value because
   every decision upstream is a function of the input sequence ([01](01-architecture.md) Rule 1).

Any SQL error fails the task and takes the same replay path. That is how a database outage is
supposed to be survived: by the job failing over and restarting until PostgreSQL is back
(fixed-delay restart, 1000 attempts, 2 s apart, `infra/stack.py`).

### Why not the JDBC connector or a transactional sink

The own sink is about 80 lines and makes the at-least-once + idempotence argument readable in one
file. It also avoids the JDBC connector's 3.x → 4.x API changes ([01](01-architecture.md)
§ Decisions). A two-phase-commit sink would hide intermediate values from readers. It would not make
the *final* state any more correct, and the final state is what is claimed. The cost of the choice
is stated as part of the claim: a crash can make a row briefly show an intermediate value, or be
written twice with the same value.

## The crash harness

`eval/run_pipeline.py` runs one scenario end to end. It generates the scenario, computes the
expected output, creates a fresh topic and consumer group, truncates the tables, submits the job and
checks the ordering chain. Then it produces input up to a cut point, injects a failure, produces
the rest, and polls PostgreSQL until the output is exact or stops changing for 20 s.

| `--crash` | What the harness does | What it exercises |
| --- | --- | --- |
| *(none)* | Produce everything | Out-of-order, late, very-late, duplicate and malformed input with no failure |
| `taskmanager` | SIGKILL the TaskManager, wait 3 s, start a new one, wait until the restart counter rises and every task runs | Automatic failover and restore from the latest checkpoint |
| `job` | Wait for a completed checkpoint, cancel the job, resubmit with `-s <checkpoint>` | Operator-driven restore; retained checkpoints; stable operator uids |
| `postgres` | Stop PostgreSQL (`pg_ctl -m fast`), wait 8 s, start it again | The sink's failure path: SQL error → task failure → replay |
| `--before-very-late` | Move the cut so the **first record after recovery is a very-late event** | The restored lateness state: without it, that event re-creates a purged window |

`eval/suite.sh` runs 16 of these with fixed seeds: 5 clean, TaskManager kills at 20/35/50/80 % of
input plus one adversarial kill, 2 job restores plus 2 adversarial, and 2 database outages.
Results are in [05](05-evaluation.md).

### Why the adversarial mode exists

It is the test that found the worst bug. Before the checkpointed lateness filter existed, a job
restore whose first replayed record was very late re-created a long-purged window. The upsert then
overwrote a 197-event row with a 1-event aggregate. `results/raw/development_runs.jsonl`, run
`20261006T095120Z-s1-job-878c` (label `adversarial-restore-no-guard`), records it: 2 windows
mismatched, e.g. `checkout` window `1767225840000` expected `request_count` 197, got 1. A clean run
or a crash at a random point would almost never put a very-late event first after a restore.

## Recovery time

Measured from the failure to "every task of the job is RUNNING on restored state", from the final
2026-10-23 suite (`results/suite.log`, `results/suite-logs/NN.log`). For a TaskManager kill it
includes the JobManager noticing the loss, read from the job's own state timestamp (entered
RESTARTING after the kill), and the 3 s before the harness starts a new TaskManager.

| Failure | Recovery | Failover noticed | Source |
| --- | --- | --- | --- |
| TaskManager SIGKILL, restarted after 3 s | 7.3–12.1 s | 2.5–6.7 s after the kill | logs 06–09, 15, 17–19, 25–27 |
| Job cancel → resume from retained checkpoint | 3.5–3.7 s idle; 2.5–5.7 s in flight or mid-checkpoint | — | logs 10–12, 16, 20–22, 28–30 |
| Sink database down for 8 s | 8.5 s idle; 9.9–10.1 s in flight, from outage start | — | logs 13, 14, 23, 24 |

**Earlier figures were measured through a 3 s cache.** Before 2026-10-23 the harness read job state
through Flink's REST views, which are cached for `web.refresh-interval` (3 s by default); `infra/stack.py`
now sets 500 ms. The old numbers (3.2–10.0 s for a TaskManager kill in the `tm-recovery-timing` runs,
8.3–8.5 s for a job restore, 9.4 s in `results/demo.log`) therefore include up to a few seconds of
cache delay, and the old TaskManager failover detector (the `numRestarts` metric) did not see the
counter rise within its 120 s cap in 7 of 11 runs of one suite, although the JobManager log shows
those jobs running again about 4 s after the kill ([09](09-implementation-notes.md) § 11b).

The sink-outage figure is mostly the outage itself: 8 s of deliberate downtime plus restart. Whether
the job failed over *during* the outage isn't recorded **[verify]**.

## Idle, in-flight and mid-checkpoint crashes

The first 16 runs inject the failure 8 s after producing the input before the cut ("input before
the cut is consumed and covered by a completed checkpoint", `eval/run_pipeline.py`). With 3 s
checkpoints and a job that drains far faster than the 4,000 records/s producer, the job is **idle**
when it fails. They show that state restore is correct, including the adversarial watermark case,
but barely use the replay-and-overwrite path (chain step 3).

Two modes added on 2026-10-23 cover the rest of PLAN.md M3's list:

- `--inflight` produces the whole input in a background thread and fires the failure once the cut
  fraction has been sent, with no pause. Records after the last completed checkpoint, many already
  written to the sink, must be replayed and overwritten with identical values.
- `--mid-checkpoint` does the same with each checkpoint held open for 2 s *after* the sink's
  pre-checkpoint flush has written its rows (the job's test-only `--checkpoint-hold-ms`, which is 0
  and does nothing in normal runs), and injects the failure only while Flink reports a checkpoint
  in progress. That is the case "rows written for a checkpoint that never completes".

Each run records the state at the moment of the failure (`results/suite-logs/17–30.log`):

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

| Log | Seed | Failure | Cut | Checkpoint in progress | Producer running | Last checkpoint age | Sink writes before crash | Recovery (s) | Exact |
|---|---|---|---|---|---|---|---|---|---|
| 25 | 31 | TaskManager kill | 30% | 1 | yes | 1.12 s | 210 | 7.3 (4.5) | yes |
| 26 | 32 | TaskManager kill | 50% | 1 | yes | 1.1 s | 382 | 8.3 (3.7) | yes |
| 27 | 33 | TaskManager kill | 80% | 1 | yes | 2.67 s | 449 | 7.8 (2.9) | yes |
| 28 | 34 | job cancel + restore | 30% | 1 | yes | 1.03 s | 217 | 3.1 | yes |
| 29 | 35 | job cancel + restore | 50% | 1 | yes | 1.12 s | 340 | 3.1 | yes |
| 30 | 36 | job cancel + restore | 80% | 1 | yes | 1.09 s | 567 | 3.1 | yes |

All 14 are exact: windows, alerts, late-dropped ids and dead-letter counts match the reference. In
every run the producer was still sending at the failure, and 36–567 window writes were already in
the sink. In all 6 mid-checkpoint runs a checkpoint was in progress with its rows written (found
within 0.02–1.06 s of the cut). TaskManager recovery shows total time and, in brackets, when the
failover was noticed.

| Still not exercised | Why it matters | What would exercise it |
| --- | --- | --- |
| Kafka broker or JobManager failure | Different recovery paths (no HA configured) | Out of scope on one host |
| Loss of the checkpoint directory or Kafka log | Not claimed | — |

Read the results together as: state restore is correct (idle, including the watermark case), replay
of processed-but-uncheckpointed records with sink overwrites is correct (in flight), and rows written
for a checkpoint that never completes are repaired by replay (mid-checkpoint)
([07](07-risks.md) § R2, [09](09-implementation-notes.md) § 11).
