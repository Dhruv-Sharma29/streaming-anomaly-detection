# 01 — Architecture

## The pipeline, and the oracle beside it

```text
 generator ──scenario file (arrival order, labels)──┬──► reference/compute.py ──► expected windows,
 (seeded)                                           │                             late ids, DLQ counts
                                                    │    detector/ewma.py ──────► expected alerts
                                                    ▼
                                    producer (idempotent, acks=all)
                                                    │
                                Kafka topic (1 partition, KRaft broker)
                                                    │
 ┌──────────────────────────────────── Flink job (checkpoint every 3 s) ─────────────────────────────────────┐
 │  [parallelism 1, one chained task]                                                                        │
 │  KafkaSource ─► validate ─► LatenessFilter ──late──► sink: late_events                                    │
 │  (per-event watermarks)  │   (checkpointed max ts)                                                        │
 │                          └─invalid─► sink: dlq                                                            │
 │                                     │ on time                                                             │
 │  [parallelism 2]                    ▼                                                                     │
 │                     Dedup (keyed by event_id, event-time TTL 5 min)                                       │
 │                                     ▼                                                                     │
 │      10 s tumbling windows (keyed by service, allowed lateness 30 s) ──► sink: windows (upsert)           │
 │                                     │      └─dropped by Flink (tripwire, must be empty)─► window_operator_late
 │                                     ▼                                                                     │
 │      EwmaDetector (keyed by service; timer at window end + lateness → finalized windows) ──► sink: alerts │
 └───────────────────────────────────────────────────────────────────────────────────────────────────────────┘
                                                    │
                                 PostgreSQL ◄───────┘  ──►  eval/run_pipeline.py: exact comparison
                                      │                     dashboard/app.py: live view
```

The generator writes one scenario file in **arrival order**. The producer replays it into Kafka;
the reference reads the same file offline. Both sides therefore see the same sequence byte for
byte, and the only thing the comparison can detect is a difference in *processing*.

## The three rules that make this design worth writing down

**Rule 1 — every decision is a function of the input sequence, not of timing.** Lateness,
de-duplication, window contents and alerts must come out the same at any replay speed and across
any crash. This is what makes an *exact* comparison with an offline reference possible at all;
with timing-dependent decisions the best you can do is "approximately right", which hides bugs.
Each of the bugs in [09](09-implementation-notes.md) § 4–8 was a place where timing leaked in.

**Rule 2 — the ordered chain runs at parallelism 1.** Source → validate → lateness is one chained
task on a single-partition topic, so processing order equals arrival order. `eval/run_pipeline.py`
(`check_ordering_chain`) fails the run if the job graph ever differs. Reason: arrival order is what
the lateness rule is defined over ([02](02-semantics.md)); a parallel validate step once reordered
records round-robin and silently changed decisions.

**Rule 3 — the sink is at-least-once, and every write is an idempotent upsert of a deterministic
value.** No transactions, no two-phase commit. A replayed write overwrites a row with the value it
already has. Rule 1 is what makes "deterministic value" true; without it, idempotent upserts would
just make the last write win. [03](03-recovery.md) spells out the argument.

## State

Everything below is checkpointed every 3 s (`--checkpoint-ms 3000` from `eval/run_pipeline.py`),
HashMap backend, file checkpoints under `.run/checkpoints/`, retained on cancellation
(`infra/stack.py`, `flink_conf_dir`).

| Operator (uid) | Parallelism | State | Why it must be restored |
| --- | --- | --- | --- |
| `source` | 1 | Kafka offsets | Replay starts exactly where the checkpoint ends |
| `lateness` | 1 | max event time seen (`ListState<Long>`) | Flink does not checkpoint watermarks; without this, a restore resets lateness to −∞ |
| `dedup` | 2 | first-seen `ts` per `event_id`, event-time timer at `ts + H` | A replayed duplicate must still be recognised |
| `windows` | 2 | window contents, "fired" flag | Late updates after a restore must land on the right row |
| `detector` | 2 | EWMA mean/variance per metric, streaks, open alert, pending window per timer | Alerts after a restore must equal the uninterrupted run |
| sinks | — | buffered statements, flushed before each checkpoint barrier | Everything before a completed checkpoint is in PostgreSQL |

## PostgreSQL tables (the sink side)

| Table | Key | Written by | Read by |
| --- | --- | --- | --- |
| `windows` | `(service, window_start_ms)` | `sink-windows` upsert; counts `writes` | comparison, dashboard, latency measurement (`first_written_at`) |
| `alerts` | `alert_id` = `service:episode_start_ms` | `sink-alerts` upsert (updates `closed_window_ms`) | comparison, dashboard |
| `late_events` | `event_id` | `sink-late`, insert-or-ignore | comparison (`late_match`) |
| `dlq` | `(kafka_partition, kafka_offset)` | `sink-dlq`, insert-or-ignore | comparison (`dlq_match`), dashboard |
| `window_operator_late` | `event_id` | tripwire side output | must be empty in every run |

Schema: `infra/stack.py` (`SCHEMA`).

## Components

| Component | Choice | Why |
| --- | --- | --- |
| Broker | Kafka 4.3.1, KRaft single node | Retains input for replay; idempotent producer prevents producer-side duplicates |
| Processor | Flink 2.2.1, DataStream API, Java 17 | Event-time windows, keyed state, checkpoints |
| State backend | HashMap, file checkpoints | State is small (minutes of windows and dedup ids) |
| Sink | PostgreSQL 16 via `UpsertSink` | Idempotent upserts keyed by `(service, window_start_ms)`, `event_id`, `(partition, offset)`, `alert_id` |
| Runtime | Local processes (`infra/stack.py`) | No container runtime on this host; same failure modes (process kill) |

## Decisions

- **Own upsert sink instead of the JDBC connector.** It buffers writes and flushes before each
  checkpoint barrier, and every statement is idempotent. About 80 lines makes the at-least-once +
  idempotence argument visible, and avoids the connector's 3.x → 4.x API changes. Cost: it's
  our code to maintain.
- **Lateness decided in a checkpointed operator**, a single partition, and per-event watermarks.
  See [02-semantics.md](02-semantics.md) for the three reproduced failures behind this.
- **Detector on finalized windows**: deterministic alerts, 30 s extra detection delay.
- **Python reference + Java job share `semantics/constants.json`.** A Java↔Python detector parity
  test requires bit-identical alerts on 10 scenarios.

## Limits

- One host: shows recovery from process failure, not node or disk loss.
- One input partition caps source throughput at a single consumer (measured in `results/`).
- Results become visible after the sink flushes, which is tied to checkpoints (measured trade-off
  in `results/`).
- The synthetic traffic has a stationary baseline, so detector comparisons don't exercise
  adaptation to slow, benign level changes.
  *Superseded by M5:* `generator/generate.py` now has an opt-in non-stationary mode (a slow
  latency wave plus benign permanent level shifts), and `results/summary/detection_budget.json`
  reports every detector on both regimes ([04](04-detection.md)). The stationary scenarios used by
  the correctness suite are unchanged byte for byte (`tests/test_detection.py`, seed-1 SHA-256).
- By default failures are injected while the job is idle (the harness waits 8 s, so the restore
  point covers all input before the cut); `--inflight` crashes while input is still arriving. A
  failure during a checkpoint is not exercised ([03](03-recovery.md) § Idle crashes and in-flight crashes).
- The learned detector is evaluated offline only; the job runs EWMA ([04](04-detection.md)).

## Repository layout

```text
semantics/   constants.json (shared by Python and Java) + parse / watermark / lateness helpers
generator/   generate.py: seeded scenarios, labels, delivery faults · produce.py: idempotent Kafka producer
reference/   compute.py: expected windows, late ids, DLQ counts; refuses ambiguous scenarios
detector/    ewma.py: Python reference of the EWMA detector (the Java copy is job/.../EwmaModel.java)
job/         Flink job, Java 17, Maven (pom.xml pins Flink 2.2.1, Kafka connector 5.0.0-2.2)
  src/main/java/pipeline/   StreamingJob, PerEventWatermarks, LatenessFilter, Dedup, WindowMetrics,
                            EwmaDetector, EwmaModel, DetectorCli (parity), EnvelopeDeserializer
  src/main/java/pipeline/sink/   UpsertSink, StatementBinder
infra/       stack.py: Postgres, Kafka (KRaft), Flink JM + TM as user-space processes, ports 19xxx
eval/        run_pipeline.py (one run + crash modes) · suite.sh (24 runs) · detection.py,
             detection_budget.py (offline detector quality) · load.py (capacity, latency)
dashboard/   app.py: FastAPI page over the Postgres tables and the Flink REST API
scripts/     setup_tools.sh (toolchain into .tools/, checksums verified) · demo.sh
tests/       52 tests: semantics, reference, generator, detector parity, feature causality
results/     suite-logs/ + suite.log (correctness) · raw/*.jsonl (every run) · summary/*.json (detectors)
```

The PLAN's target layout had separate `sink/` and `chaos/` directories; the sink lives in
`job/.../sink/` and crash injection in `eval/run_pipeline.py` (`crash`).
