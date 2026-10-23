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

Measured from the failure to "every task of the job is RUNNING on restored state". It covers the
JobManager noticing the loss, which is bounded by the 10 s heartbeat timeout in `infra/stack.py`.

| Failure | Recovery | Source |
| --- | --- | --- |
| TaskManager SIGKILL, restarted after 3 s | 3.2, 10.0, 10.0 s | `results/raw/pipeline_runs.jsonl`, label `tm-recovery-timing` (seeds 17–19) |
| same, during the demo | 9.4 s | `results/demo.log` |
| Job cancel → resume from retained checkpoint | 8.0–8.1 s idle; 4.9–5.1 s in flight | `results/suite-logs/10–12,16,20–22,28–30.log` (2026-10-23) |
| Sink database down for 8 s | 8.4 s idle; 11.5–11.6 s in flight, from outage start | `results/suite-logs/13,14,23,24.log` (2026-10-23) |

The TaskManager timings in the 2026-10-23 suite are not usable: with the fixed timer (wait for
`numRestarts` to rise, capped at 120 s), detected failover ranged from 3.1 s to the cap for the same
failure, and 7 of 11 TaskManager runs hit the cap, while every run's output was already exact. The
suite ran next to two GPU training jobs; the cause is not established. The three
`tm-recovery-timing` runs (seeds 17–19) remain the reference; one later run, `portability-regression`
(seed 3, same file), took 11.0 s. Next check: the JobManager log's restart timestamps for a capped run.

The sink-outage figure is mostly the outage itself: 8 s of deliberate downtime plus restart.
Whether the job actually failed over *during* the outage isn't recorded. The input is paused at
that moment (next section), so the sink may have had nothing to write until the outage ended
**[verify]** (`numRestarts` is not logged for this mode).

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
| 17 | 21 | TaskManager kill | 30% | 0 | yes | 0.27 s | 174 | 9.4 | yes |
| 18 | 22 | TaskManager kill | 50% | 0 | yes | 0.99 s | 262 | 127.2 ‡ | yes |
| 19 | 23 | TaskManager kill | 80% | 0 | yes | 0.55 s | 491 | 123.3 ‡ | yes |
| 20 | 24 | job cancel + restore | 30% | 0 | yes | 0.8 s | 145 | 5.1 | yes |
| 21 | 25 | job cancel + restore | 50% | 0 | yes | 0.49 s | 308 | 5.1 | yes |
| 22 | 26 | job cancel + restore | 80% | 0 | yes | 1.32 s | 497 | 5.0 | yes |
| 23 | 27 | sink database down 8 s | 40% | 0 | yes | 0.27 s | 253 | 11.6 | yes |
| 24 | 28 | sink database down 8 s | 70% | 0 | yes | 2.19 s | 351 | 11.5 | yes |

| Log | Seed | Failure | Cut | Checkpoint in progress | Producer running | Last checkpoint age | Sink writes before crash | Recovery (s) | Exact |
|---|---|---|---|---|---|---|---|---|---|
| 25 | 31 | TaskManager kill | 30% | 1 | no | 1.0 s | 588 | 123.1 ‡ | yes |
| 26 | 32 | TaskManager kill | 50% | 1 | no | 1.03 s | 619 | 123.4 ‡ | yes |
| 27 | 33 | TaskManager kill | 80% | 1 | yes | 1.36 s | 535 | 123.1 ‡ | yes |
| 28 | 34 | job cancel + restore | 30% | 1 | yes | 4.76 s | 17 | 8.1 | yes |
| 29 | 35 | job cancel + restore | 50% | 1 | yes | none completed yet | 247 | 8.0 | yes |
| 30 | 36 | job cancel + restore | 80% | 1 | yes | 1.2 s | 535 | 4.9 | yes |

All 14 are exact: windows, alerts, late-dropped ids and dead-letter counts match the reference. In
the 8 in-flight runs the producer was still sending and 145–497 window writes were already in the
sink. In all 6 mid-checkpoint runs a checkpoint was in progress with its rows written; in runs 25–26
the wait for one outlasted the producer, so those two are mid-checkpoint but not mid-input.
Recovery marked ‡ hit the failover detector's cap ([§ Recovery time](#recovery-time)).

| Still not exercised | Why it matters | What would exercise it |
| --- | --- | --- |
| Kafka broker or JobManager failure | Different recovery paths (no HA configured) | Out of scope on one host |
| Loss of the checkpoint directory or Kafka log | Not claimed | — |

Read the results together as: state restore is correct (idle, including the watermark case), replay
of processed-but-uncheckpointed records with sink overwrites is correct (in flight), and rows written
for a checkpoint that never completes are repaired by replay (mid-checkpoint)
([07](07-risks.md) § R2, [09](09-implementation-notes.md) § 11).
