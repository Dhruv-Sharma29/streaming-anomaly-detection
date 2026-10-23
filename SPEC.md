# 05 — Streaming anomaly detection with crash recovery

**Build fifth in the general portfolio order.** Move this after project 1 if cloud/systems engineering becomes your main target.

## Objective

Build a streaming pipeline that computes live service metrics, detects anomalies, and preserves correct results through duplicate events, out-of-order delivery, replay, and worker failure.

The central engineering question is correctness under failure. The anomaly detector is a separate component you can evaluate and improve.

## Why this fits Amazon and Microsoft

[AWS Managed Service for Apache Flink](https://docs.aws.amazon.com/managed-flink/latest/apiv2/Welcome.html) supports streaming processing and analytics. [Microsoft Fabric Real-Time Intelligence](https://learn.microsoft.com/en-us/fabric/real-time-intelligence/overview) combines ingestion, analysis, visualization, and actions on streaming data.

2028+ assessment: stateful processing, backpressure, schema changes, and recovery should remain relevant across AI and conventional backend systems.

## Minimum viable scope

- A deterministic event generator with ground-truth anomaly labels.
- A broker, one stateful processing job, a sink, and a dashboard.
- Event-time windows, explicit late-event handling, and deduplication policy.
- A simple statistical anomaly detector and optionally a trained detector.
- Checkpoint/restart and replay validation against an independent reference calculation.

## Proposed stack and architecture

Kafka, Flink, PostgreSQL as an initial sink, Python for fixtures/evaluation, and a lightweight dashboard. Choose a Flink language/runtime supported by your chosen Arm-compatible environment; Java is a reasonable starting choice. Terraform is a later cloud extension.

```text
Seeded events -> Kafka -> Flink windows/features -> Detector -> Sink/dashboard
                              |
                         Checkpoint state
```

Keep the sink simple initially. Introduce a specialized analytical database only if measurements show a need.

## Implementation stages

1. **Reference calculation:** generate repeatable events and compute expected metrics offline. Exit when expected results are unambiguous.
2. **Streaming baseline:** implement aggregation and sink writes without ML. Validate normal and out-of-order events.
3. **Failure correctness:** add restart, checkpoints, sink idempotency or supported transactional handling, and replay tests.
4. **Anomaly baseline:** use moving statistics or a simple rule. Label detection time and false alerts.
5. **Optional learned detector:** train on separate runs and compare under the same alert budget.
6. **Load and deployment:** measure backpressure and recovery locally. Add a budgeted AWS or Azure/Fabric variant if it supports your intended demonstration.

## Data and evaluation

Generate service latency, errors, queue depth, and throughput events. Inject spikes, gradual changes, outages, duplicates, delayed events, schema changes, and benign traffic shifts. Use multiple seeds and separate training/testing time periods.

Measure successful events per second, event-to-result latency, recovery duration, output correctness after replay, anomaly precision/recall, and detection delay. Fix workload and hardware settings when comparing versions.

Define event time versus ingestion time, watermark policy, deduplication horizon, and what happens after that horizon. Avoid claiming end-to-end exactly-once behavior without validating the broker, processor, and sink together.

## Important tests

- Replayed event IDs do not inflate counts inside the specified deduplication horizon.
- Late events produce the explicitly documented update/drop behavior.
- A processing worker crashes before/after sink writes; final results match the reference under the stated guarantees.
- Backpressure remains visible and bounded rather than silently dropping data.
- Malformed or incompatible schema versions go to a visible error/dead-letter path.
- Repeated anomaly events do not create uncontrolled duplicate alerts.

## GX10 and deployment

This project uses mostly CPU/RAM. Establish correctness without a GPU model. Local containers demonstrate process failure, not physical-host resilience. Record Arm image compatibility, resource limits, and checkpoint storage behavior.

Cloud work should include spending limits, resource inventory, and teardown. Do not assume a managed service preserves your local guarantees automatically.

## Definition of done

The demo injects an anomaly, duplicates and delays events, kills a worker, restores processing, and verifies final outputs against the reference. Publish detection quality and system recovery results separately.

Deliver the event generator, reference calculator, job code/configuration, crash scripts, evaluation reports, dashboard, and architecture diagram. Résumé template: “Built a stateful streaming pipeline processing [measured load]; validated [recovery/correctness result] and measured [detector quality] under injected failures.”
