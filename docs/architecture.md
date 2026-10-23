# Architecture

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
  See `semantics.md` for the three reproduced failures behind this.
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
