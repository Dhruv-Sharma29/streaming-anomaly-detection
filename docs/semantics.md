# Stream semantics

The reference calculator (`reference/`) and the Flink job (`job/`) implement these rules exactly.
If they disagree, either the job is wrong or this document is ambiguous. Fix the document first.

## Events

```json
{"schema_version": 1, "event_id": "s3-000123456", "service": "checkout", "ts_ms": 1767225600123,
 "latency_ms": 41.7, "status": 200, "queue_depth": 12}
```

| Field | Rule |
| --- | --- |
| `schema_version` | 1, or 2 (v1 plus optional `region`). Anything else → dead-letter |
| `event_id` | Unique per logical event. A duplicate delivery repeats it exactly |
| `service` | Non-empty string; also the Kafka key |
| `ts_ms` | **Event time**: when the request happened at the service (epoch ms) |
| `latency_ms` | Number ≥ 0 |
| `status` | Integer HTTP status; ≥ 500 counts as an error |
| `queue_depth` | Integer ≥ 0 |

Malformed JSON, a missing or mistyped required field, or an unknown `schema_version` sends the raw
record to the `events-dlq` topic with a reason, and it's counted. It's never silently dropped.

**Ingestion time** is the Kafka record timestamp (broker append time). It's recorded for latency
measurement only and is never used for windowing.

## Windows and metrics

Tumbling **10 s event-time windows** aligned to the epoch, keyed by `service`. Per window:

| Metric | Definition |
| --- | --- |
| `request_count` | accepted, de-duplicated events in the window |
| `error_count` | of those, `status >= 500` |
| `latency_p95_ms` | nearest-rank 95th percentile of `latency_ms` |
| `queue_depth_max` | maximum `queue_depth` |

A window row is identified by `(service, window_start_ms)`; the sink upserts on that key.

## Watermark and lateness

- **Watermark** = (max event time seen so far, in arrival order) − **B** − 1 ms, B = 5 s (the same
  formula as Flink's `BoundedOutOfOrdernessWatermarks`).
- **Allowed lateness L = 30 s.** An event is applied to its window while
  `window_end − 1 ms + L > watermark` (watermark taken *before* the event). Updates after the
  window's first firing upsert the row again.
- Otherwise it's **late-dropped**: recorded in the `late_events` table with its window, so drops
  stay visible and countable.

### Who decides lateness, and why not Flink's window operator

The job decides lateness in its own operator (`LatenessFilter`), using the formula above with the
max event time held in **checkpointed operator state**. Flink's window operator still windows, but
never drops anything: its watermark is never ahead of the filter's. A tripwire table
(`window_operator_late`) records anything it does drop, and every evaluation requires it to be empty.

This design came out of three failures, each reproduced before being fixed:

1. **Periodic watermarks lag at replay speed.** At ~46× real time, Flink's default 200 ms emission
   let the watermark trail by ~9 s of event time. Fix: emit a watermark after every event.
2. **Partition skew changes decisions.** With 3 partitions, Flink's watermark is the minimum across
   partitions. During start-up catch-up one partition lagged far behind, and Flink applied 8
   very-late events the reference dropped. Fix: a single partition, with source → validate →
   lateness chained at parallelism 1 so processing order equals arrival order. The job graph is
   checked on every run, because a parallel validate step once silently reordered records.
3. **Flink doesn't checkpoint watermarks.** After a restore, the window operator's watermark
   restarts at −∞. A very-late event replayed first then re-created its long-purged window, and the
   upsert overwrote a 197-event row with a 1-event aggregate (`eval.run_pipeline --crash job
   --before-very-late`). Fix: the checkpointed lateness filter above.

With all three in place, the job's lateness decisions are a function of the input sequence alone,
before and after recovery.

**Scaling beyond one partition** (not implemented): use per-partition lateness (define arrival
order per partition), or Flink watermark alignment plus a decision margin larger than the
alignment drift. Either way the reference model has to change too, because a single global
arrival order no longer exists.

### Decision margin

The reference still refuses any decision within **15 s** of a boundary where Flink's own
event-time timers matter: dedup-state expiry, and late-drop as a second line of defence. The
generator stays clear by construction (delays: normal 0–3 s, late 25–32 s, very late 75–110 s;
no late classes in the final 130 s, where the watermark stops advancing). Tests check 20 seeds.

## De-duplication

- Keyed by `event_id`, horizon **H = 5 min of event time**: an `event_id` seen before is dropped
  until the watermark passes `first_ts + H`, after which its state is cleared.
- De-duplication happens before windowing, so duplicates can't inflate any window.
- After the horizon, a re-delivered event would be accepted again. Such an event is necessarily
  more than L behind the watermark, so it's late-dropped instead. In this design a duplicate can
  never inflate a final count. The generator also keeps duplicate re-delivery well inside H.

## Delivery and correctness guarantee

Claimed: **after any crash and recovery of the processing job, the final contents of the
`windows` table equal the reference.** This rests on three things, all tested:

1. Kafka retains the input and Flink commits offsets only in checkpoints, so input is replayed
   from the last checkpoint (at-least-once input into the job).
2. Window, de-duplication, lateness-watermark and detector state are restored from the same
   checkpoint, consistent with those offsets.
3. Sink writes are idempotent upserts of deterministic values, so a replayed write overwrites the
   row with the same value.

Not claimed:
- **End-to-end exactly-once side effects.** A crash can cause a row to be written twice with the
  same value, or an intermediate value to be visible briefly before the replay completes.
- Correctness of **alerts consumed by external systems** beyond the deterministic `alert_id` upsert.
- Durability across **loss of the Kafka log or checkpoint storage**. One host demonstrates process
  failure only.

Tested by `eval/suite.sh`: clean runs, TaskManager SIGKILL at several points, job cancel and
restore from checkpoint, sink database restart, and adversarial restores where the first record
after recovery is very late. Every run must match the reference exactly: windows, late-dropped
ids, dead-letter counts by reason, and alerts.

## Anomaly detection

Separate from correctness. The detector runs on **finalized** windows: an event-time timer at
`window max timestamp + L` processes the last value of each window. The first version used first
firings. Their contents depend on how far the watermark lagged when late events arrived, so
alerts differed between runs of identical input. Final values are deterministic, which makes
alerts replay-safe. Cost: L (30 s) of extra detection delay. See `detector/` for the rule and
`results/summary/detection.json` for quality.
