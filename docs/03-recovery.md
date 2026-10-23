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
| Job cancel → resume from retained checkpoint | 8.3–8.5 s | `results/suite-logs/10,11,12,16.log` |
| Sink database down for 8 s | 8.4 s from outage start | `results/suite-logs/13,14.log` |

The TaskManager numbers in `results/suite-logs/06–09,15.log` (3.1, 10.0, 9.4, 9.9, 3.1 s) were
recorded with an earlier timer that could stop before the JobManager had even noticed the loss
(the fields there are `running_after_restart_s`, not `failover_detected_s`). The code comment in
`crash()` names 3.1 s as the spurious value. The three `tm-recovery-timing` runs were made with the
fixed timer for that reason. One later run, `portability-regression` (seed 3, same file), took
11.0 s.

The sink-outage figure is mostly the outage itself: 8 s of deliberate downtime plus restart.
Whether the job actually failed over *during* the outage isn't recorded. The input is paused at
that moment (next section), so the sink may have had nothing to write until the outage ended
**[verify]** (`numRestarts` is not logged for this mode).

## What the suite does not exercise

`run_once` sleeps 8 s after producing the input before the cut and before injecting the failure
("input before the cut is consumed and covered by a completed checkpoint", `eval/run_pipeline.py`).
With 3 s checkpoints and a job that drains far faster than the 4,000 records/s producer, the job is
**idle** when it fails, and the restore point already covers all input before the cut. So:

| Not exercised | Why it matters | What would exercise it |
| --- | --- | --- |
| Kill while records are in flight | The replay-and-overwrite path (chain step 3) is barely used | Crash without the pause, at random offsets, while producing |
| Kill between a sink flush and checkpoint completion | Rows written for a checkpoint that never completes must be overwritten identically | Crash triggered on the checkpoint-start notification |
| Kill mid-checkpoint | PLAN.md M3 asks for it explicitly | Same, with a slow checkpoint |
| Kafka broker or JobManager failure | Different recovery paths (no HA configured) | Out of scope on one host |
| Loss of the checkpoint directory or Kafka log | Not claimed | — |

The adversarial cases do attack restored state in the place where it was actually broken. Read
the 16/16 result as "state restore is correct, including the watermark", not as "replay under
load is correct". The latter is argued (chain step 3) but only lightly tested. This is listed as
[07](07-risks.md) § R2 and [09](09-implementation-notes.md) § 11.
