# 08 — Learning checklist

Ordered by the milestone that forces each skill. "Done when" points at something in this repo.

## M1 — Reference calculation

| Skill | Why | Done when |
|---|---|---|
| Event time vs ingestion time; bounded out-of-orderness watermarks | Every later decision is defined in event time | You can derive `watermark = max_ts − B − 1 ms` and say why the −1 ms is there (`semantics.watermark`) |
| Allowed lateness and Flink's `isWindowLate` boundary | Off-by-one here changes which events are dropped | `test_late_slack_matches_flink_is_window_late` passes and you can explain both asserts |
| Seeded, replayable test data with ground truth | An oracle needs identical input on both sides | `test_generator_is_deterministic`; the seed-1 SHA-256 in `tests/test_detection.py` |
| Refusing ambiguous input instead of guessing | A comparison is only exact if the expected value is unambiguous | `test_drop_decisions_too_close_to_the_boundary_are_refused` |

## M2 — Streaming baseline

| Skill | Why | Done when |
|---|---|---|
| Kafka basics: partitions, keys, idempotent producer, `acks=all` | Producer retries must not add duplicates the reference doesn't know about | `generator/produce.py` and why records are keyed by `event_id` |
| Flink DataStream: sources, `keyBy`, tumbling windows, `ProcessWindowFunction`, side outputs | The job | `StreamingJob.java` reads top to bottom without surprises |
| Custom `WatermarkGenerator` | The default periodic emission lagged ~9 s at replay speed | `PerEventWatermarks.java`, and you can say what lag it removes |
| Operator chaining and parallelism | Processing order = arrival order only inside one chained task | `check_ordering_chain` in `eval/run_pipeline.py` |

## M3 — Failure correctness

| Skill | Why | Done when |
|---|---|---|
| Checkpoint barriers and what state is (and isn't) in a checkpoint | Watermarks aren't. That was the worst bug | You can explain why `LatenessFilter` keeps `max-ts` in `ListState` |
| `CheckpointedFunction` operator state vs keyed state | Lateness state is per operator, dedup and EWMA state per key | `LatenessFilter` vs `Dedup` / `EwmaDetector` |
| Sink V2 `SinkWriter.flush` before a barrier | The at-least-once half of the guarantee | `UpsertSink.java` |
| Idempotent upserts (`ON CONFLICT … DO UPDATE / DO NOTHING`) | The other half | The five tables in [01](01-architecture.md) and their keys |
| Restart strategies, heartbeats, retained checkpoints, stable uids | Recovery time and restore-by-name | `flink_conf_dir` in `infra/stack.py`; every operator has `.uid(...)` |
| Fault injection that targets the weak spot | Random crash points don't find the watermark bug | `--before-very-late` and the `adversarial-restore-no-guard` run |

## M4–M5 — Detection

| Skill | Why | Done when |
|---|---|---|
| EWMA mean/variance, z-scores with variance floors | The in-job detector | `detector/ewma.py` and its two floors |
| Cross-language floating-point parity | Bit-identical alerts need identical operation order | `tests/test_detector_parity.py` passes |
| Event-based scoring: matching, delay, false alerts per run | Window-level accuracy misleads for episodes | `score` in `eval/detection.py` |
| Fixed-budget comparison, out-of-fold threshold selection | Comparing detectors at their own best settings is unfair; tuning on training data looks perfect | `choose` and `OutOfFold` in `eval/detection_budget.py` |
| Causal features and a test for them | Future leakage is the usual way a learned detector cheats | `test_features_use_only_past_windows` |
| Bootstrap intervals over scenarios | 30 runs is small, and differences need intervals | `bootstrap_diff` |

## M6 — Load

| Skill | Why | Done when |
|---|---|---|
| Flink REST views: backpressure, busy ratio, job state timestamps, and their caching (`web.refresh-interval`) | Visible backpressure, honest recovery timing | `backpressure_sampler` in `eval/load.py`; `crash()` reads the RESTARTING timestamp ([09](09-implementation-notes.md) § 11b, 11d) |
| Latency vs flush-interval trade-off | Result staleness is set by the sink, not the job | The two latency runs in [05](05-evaluation.md) |

## Interview talking points, mapped to what was built

| Point | Evidence |
|---|---|
| "I compared the job with an offline oracle *exactly*, not approximately, and that's what found the bugs." | Five pipeline bugs, each reproduced first ([09](09-implementation-notes.md) § 4–8) |
| "Flink doesn't checkpoint watermarks. After a restore, one very-late event overwrote a 197-event window with a 1-event aggregate." | `development_runs.jsonl`, `adversarial-restore-no-guard`; fix in `LatenessFilter` |
| "At-least-once plus deterministic idempotent upserts gives exact final state. Here is what it doesn't give." | [03](03-recovery.md); [07](07-risks.md) § The question to have a real answer for |
| "I gave up 30 s of alert delay for replay-safe alerts." | `first-e2e` run: windows exact, alerts not; [04](04-detection.md) |
| "My first detector comparison didn't separate detectors, so I made the data harder and compared at a fixed budget." | `EFFECTS` comment; M4 vs M5 in [05](05-evaluation.md) |
| "The learned detector's win is fewer false alerts on benign shifts, not recall." | Bootstrap intervals in `detection_budget.json` |
| "The first suite killed an idle job, so I added crashes mid-stream and inside a held-open checkpoint: 14/14 exact with sink writes replayed." | [03](03-recovery.md), [06](06-roadmap.md) item 1 |
