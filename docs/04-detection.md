# 04 — Anomaly detection

Detection is a separate component with a separate result. It consumes the window stream that
[03](03-recovery.md) shows to be exact, and it must never be used to hide a correctness problem
(PLAN.md: "Don't let (2) hide weaknesses in (1)").

## Labels: what counts as an anomaly

`generator/generate.py` places 5 anomalies and 2 benign traffic shifts per 10-minute scenario,
after a 120 s warm-up, aligned to windows, non-overlapping per service, each 30–90 s long.
Severity is uniform in [0, 1] and scales the effect size linearly:

| Kind | Effect at severity 0 → 1 |
| --- | --- |
| `latency_spike` | latency × 1.1 → × 6.0 |
| `error_burst` | +0.5 % → +30 % extra 5xx |
| `outage` | 3 % → 85 % of requests fail slowly (503, 1.5–3 s) |
| `latency_drift` | extra latency rising to +15 % → +400 % by the end |
| `queue_growth` | queue depth +4 → +600 by the end |
| `traffic_shift` (benign) | 2× request rate; alerts here count as false |

The low end was widened after a first version where a static threshold reached 99 % held-out
recall and the evaluation could not separate detectors (comment on `EFFECTS`).

**Non-stationary mode** (`Config(nonstationary=True)`, added in M5): a ±25 % latency wave with a
300 s period, plus 2 benign *permanent* level shifts per run (latency × 1.3–1.6 from a random point
to the end). It's driven by a separate RNG stream, so stationary scenarios stay byte-identical
(`tests/test_detection.py` pins the seed-1 SHA-256).

## The in-job detector: EWMA z-score

`detector/ewma.py` (Python reference) and `job/.../EwmaModel.java` (Flink), constants from
`semantics/constants.json`:

| Setting | Value | Meaning |
| --- | --- | --- |
| Metrics | `latency_p95_ms`, `error_rate`, `queue_depth_max` | per service, per window |
| `alpha` | 0.1 | EWMA weight for mean and variance |
| `warmup_windows` | 6 | baseline-only windows per service |
| `z_threshold` | 4.0 | a window is hot if any metric's upward z exceeds it |
| `std_floor` | latency max(5 ms, 10 % of mean); error rate 0.01; queue max(2, 10 % of mean) | stops a near-constant baseline from making z explode |
| `open_after` / `close_after` | 2 / 3 | consecutive hot windows to open an alert, normal windows to close it |

The baseline updates during warm-up and on **normal windows only**, so an anomaly is not absorbed
into its own baseline. The flip side is measured in M5: a permanent benign level shift looks like
an anomaly that never ends. An alert is identified by `service:episode_start_ms` and upserted,
so a replay rewrites the same row and closing an alert updates it in place.

### Why it runs on finalized windows

`EwmaDetector.java` buffers each window's latest result and processes it on an event-time timer at
`window max timestamp + L`, when no further update can arrive. The first version ran on first
firings. The first end-to-end run matched every final window but produced different alerts:
`results/raw/development_runs.jsonl`, label `first-e2e`, `windows_mismatched: 0`,
`alerts_match: false`. A first firing's contents depend on how far the watermark lagged when late
events arrived, and that depends on replay speed. Final values don't. The price is L = 30 s of
extra emission delay, on top of the decision delay reported below.

### Java ↔ Python parity

`tests/test_detector_parity.py` runs the compiled jar's `DetectorCli` on the reference's final
windows for seeds 1–10 and requires the alert lists to be **equal**, including `z_at_open` as a
double. That only works because both implementations perform the same floating-point operations in
the same order. It needs the jar and the JDK and is skipped otherwise. It passed in the
2026-10-23 run of `pytest` (52 passed, 0 skipped).

## The two offline comparators

| Detector | Rule | Knob |
| --- | --- | --- |
| Static | per service: p95 > a × warm-up median p95, or error rate > b, or queue max > c × warm-up median queue (`eval/detection.py`, `static_run`) | (a, b, c) |
| Learned | `HistGradientBoostingClassifier(max_iter=300, learning_rate=0.05, random_state=0)` on 17 causal features per window: for each of p95, error rate, queue max and request count, the ratio to the median of the last 6 and last 30 windows, a z-score against the last 30, and the delta to the previous window; plus the raw error rate (`eval/detection_budget.py`) | probability threshold |

All three share the open/close rule. The learned model is trained on window labels (a window is
positive if it starts inside an anomaly interval), on dev scenarios only: 8,640 windows, 14.2 %
positive (`results/summary/detection_budget.json`). `tests/test_detection.py` checks that the
features use past windows only, by rewriting the future and asserting the earlier features don't
change.

## Evaluation protocol

| Item | Rule | Source |
| --- | --- | --- |
| Input | Reference final windows, not cluster runs | `eval/detection*.py` |
| Dev / held-out | seeds 1–20 / 101–130 | same |
| Match | an alert matches an anomaly on the same service if it opens within [start, end + 30 s] | `eval/detection.py`, `score` |
| False alert | any unmatched alert; those during a benign interval also counted separately | same |
| Delay | end of the opening window − label start (event time) | same |
| Fixed budget (M5) | each knob chosen on dev, both regimes pooled, to maximise recall with ≤ 0.5 false alerts per run, then frozen | `eval/detection_budget.py`, `choose` |
| Learned knob | chosen on **out-of-fold** dev predictions (two models on disjoint halves of dev), then the final model is trained on all dev | `OutOfFold` |
| Uncertainty | 95 % bootstrap intervals over held-out scenarios, 2,000 resamples | `bootstrap_diff` |

Two details to quote correctly:
- The out-of-fold split in the code is by **seed parity** (`seed % 2`). The module docstring says
  "dev seeds 1–10 … 11–20". The code is what ran ([09](09-implementation-notes.md) § 16).
- Under the fixed budget, the EWMA knob chosen was **z = 3.5** (`detection_budget.json`,
  `detectors.ewma.knob`), while the Flink job runs z = 4.0. The README's "runs in the Flink job"
  row is the job's *algorithm* at a re-tuned threshold. The deployed configuration's quality is the
  M4 result in `results/summary/detection.json`, stationary traffic only
  ([09](09-implementation-notes.md) § 13).

**Why offline is legitimate here.** The correctness suite shows the job writes exactly the
reference's windows and exactly the Python detector's alerts on the seeds it ran. Re-running 30 × 2
held-out scenarios on the cluster would measure the same numbers more slowly. The limit of that
argument: every cluster run used stationary scenarios. Exactness on non-stationary input is
inferred, because the semantics don't depend on metric values, not observed.

Results: [05](05-evaluation.md) § Detection quality.
