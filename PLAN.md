# 05 — Streaming anomaly detection with crash recovery: execution plan

Brief: [SPEC.md](SPEC.md). Two separately reported results: **(1) pipeline correctness under failure**, **(2) detector quality**. Don't let (2) hide weaknesses in (1).

**Needs:** nothing (no GPU). Reuses the telemetry shape from 02 if available.

## Target layout

```text
05-streaming-anomaly-detection/
  generator/           seeded event generator + ground-truth anomaly labels
  reference/           offline reference calculator (Python) — the oracle
  job/                 Flink job (Java to start), windowing, dedup, detector hook
  sink/                Postgres schema with idempotent upsert keys
  detector/            statistical baseline; optional learned detector
  chaos/               kill/restart scripts, duplicate/delay injectors, schema-change injector
  dashboard/           lightweight live view
  infra/               compose: Kafka (KRaft), Flink JM/TM, Postgres — arm64 images
  eval/  results/raw/  results/summary/  docs/
```

## Progress (2026-10-06)

| Milestone | State |
| --- | --- |
| M1 reference calculation | **Done.** Seeded generator with labels and delivery faults; reference refuses ambiguous scenarios; 20 seeds tested |
| M2 streaming baseline | **Done.** Kafka 4.3.1 (KRaft) → Flink 2.2.1 → Postgres, exact vs reference |
| M3 failure correctness | **Done.** 30/30 suite runs exact (2026-10-23): with the job idle, while input arrives, and inside a checkpoint; TaskManager kill, job restore, sink restart, adversarial restores |
| M4 anomaly baseline | **Done.** EWMA detector (Java = Python, bit-identical); held-out P/R/delay; static-threshold comparison |
| M5 learned detector | **Done (offline).** Non-stationary traffic mode added; gradient-boosted detector vs EWMA vs static under a fixed false-alert budget: significantly fewer false alerts on non-stationary traffic |
| M6 load + recovery | **Done** locally (see README). Cloud variant not done (optional, needs a budget) |

Deviations from this plan, documented in [`docs/02-semantics.md`](docs/02-semantics.md) and [`docs/01-architecture.md`](docs/01-architecture.md) (full list: [`docs/09-implementation-notes.md`](docs/09-implementation-notes.md)):
- Single input partition plus a checkpointed lateness operator, instead of relying on Flink's
  per-partition watermarks and window-operator lateness (three reproduced failures).
- Detector consumes finalized windows, not first firings (deterministic alerts, +30 s delay).
- Own idempotent JDBC upsert sink, not the JDBC connector.
- Local processes instead of containers (no Docker on the host).

## Write these definitions first ([`docs/02-semantics.md`](docs/02-semantics.md))
- Event time vs ingestion time; which one windows use.
- Watermark policy (e.g. bounded out-of-orderness of X s) and allowed lateness.
- Late-event behaviour: update window result vs side-output/drop — pick one.
- Dedup key and horizon; what happens to duplicates arriving after the horizon.
- The end-to-end guarantee you claim and the components it depends on (Kafka offsets + Flink checkpoints + sink idempotency/transactions).

## Milestones

### M1 — Reference calculation (1 week)
- [x] Generator: latency, errors, queue depth, throughput per service; seeded.
- [x] Injected: spikes, gradual drift, outages, duplicates, delayed events, schema changes, benign traffic shifts. Labels stored separately.
- [x] Reference calculator computes the expected windowed metrics using the semantics doc.
- **Exit:** expected results are unambiguous for every seed.

### M2 — Streaming baseline (1–2 weeks)
- [ ] Verify arm64 images for Kafka, Flink, Postgres. (Not applicable as built: no container runtime on the host, so all three run as local arm64 processes; docs/09.)
- [x] Aggregation job → Postgres. Compare output to reference for in-order and out-of-order streams.

### M3 — Failure correctness (2 weeks) — the core of the project
- [x] Enable checkpoints; idempotent upsert keyed on (metric, window, key) or Flink-supported transactional sink.
- [x] Crash scripts: kill TaskManager before sink write, after sink write, mid-checkpoint. (`--inflight` and `--mid-checkpoint`, 2026-10-23.)
- [x] Replay from earlier offsets; dedup within horizon.
- [x] Schema change / malformed event → dead-letter topic, visible on dashboard. (A PostgreSQL `dlq` table, not a topic: docs/09 § 2.)
- **Exit:** after every crash scenario, final sink output == reference under the stated guarantee.

### M4 — Anomaly baseline (1 week)
- [x] Moving mean/std or EWMA + threshold. Alert dedup/cool-down so one anomaly ≠ alert storm.
- [x] Metrics: precision, recall, detection delay, false alerts per hour.

### M5 — Optional learned detector
Train on separate seeds/time periods; compare at the **same alert budget**.

### M6 — Load + optional cloud
- [x] Throughput, event-to-result latency, recovery duration; backpressure visible and bounded. (Backpressure via a slow-sink run, 2026-10-23: docs/05.)
- [ ] Cloud variant only with spending cap, resource inventory, teardown script. Don't assume managed service = same guarantees.

## Tests
- [x] Replayed IDs don't inflate counts in-horizon · [x] late events follow documented rule · [x] crash before/after sink write matches reference · [x] backpressure bounded, no silent drop · [x] bad schema → DLQ · [x] no duplicate alert storms

## Risks
- Flink on arm64 in containers — check early.
- "Exactly-once" is easy to over-claim. Only claim what M3 tests demonstrate.

## Definition of done
- [x] Demo: inject anomaly + duplicates + delays, kill worker, recover, verify vs reference · [x] correctness results · [x] detector results (separately) · [x] architecture diagram
