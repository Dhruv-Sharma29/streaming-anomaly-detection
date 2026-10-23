# 06 — Roadmap

PLAN.md is the original execution plan. This doc records how each milestone closed, with the
evidence, and what is left. Status as of 2026-10-23. The last recorded runs are from 2026-10-06.

## Milestones

| Milestone | Exit criterion (PLAN.md) | State | Evidence |
| --- | --- | --- | --- |
| M1 Reference calculation | Expected results unambiguous for every seed | **Done** | `reference/compute.py` raises `AmbiguousDecision`; `tests/test_reference.py` over 20 seeds |
| M2 Streaming baseline | Output equals reference for in-order and out-of-order streams | **Done** | `results/raw/development_runs.jsonl` → `e2e-single-partition` exact; clean runs 01–05 |
| M3 Failure correctness | After every crash scenario, final output equals reference | **Done** | 30/30 exact: 16 with the job idle, 8 while input arrives, 6 inside a checkpoint with its rows written ([05](05-evaluation.md), [03](03-recovery.md)) |
| M4 Anomaly baseline | EWMA + threshold, alert dedup, P/R/delay/false alerts | **Done** | `results/summary/detection.json`; parity test; open/close rule |
| M5 Learned detector (optional) | Separate training runs, same alert budget | **Done, offline** | `results/summary/detection_budget.json`. Not deployed in the job |
| M6 Load + optional cloud | Throughput, latency, recovery; backpressure visible and bounded | **Local part done**; cloud not done | `results/raw/load_runs*.jsonl`; slow-sink run 2026-10-23: backpressure visible (0.95–0.98), output exact |

### PLAN.md test list

| Test | State |
| --- | --- |
| Replayed ids don't inflate counts in-horizon | Covered: dedup + exact windows in every run |
| Late events follow the documented rule | Covered: `late_match` + boundary unit tests |
| Crash before/after sink write matches reference | Covered: 8 in-flight runs (sink writes past the last checkpoint replayed) and 6 mid-checkpoint runs, all exact |
| Backpressure bounded, no silent drop | Covered: slow-sink run, backpressure 0.95–0.98, exact over 2.5 M records |
| Bad schema → DLQ | Covered: `dlq` table, per-reason counts |
| No duplicate alert storms | Covered: open/close rule, `alert_id` upsert, exact alerts |

### Definition of done (PLAN.md)

| Item | State |
| --- | --- |
| Demo: inject anomaly + duplicates + delays, kill worker, recover, verify vs reference | Done: `scripts/demo.sh` → `results/demo.log` (seed 42, exact) |
| Correctness results | Done: [05](05-evaluation.md) |
| Detector results, separately | Done: [05](05-evaluation.md) § Detection quality |
| Architecture diagram | Done: [01](01-architecture.md), README mermaid diagram |

## Next, in order of what it would buy

1. ~~**Crash during processing and inside a checkpoint.**~~ Done 2026-10-23 (`--inflight` 8/8,
   `--mid-checkpoint` 6/6). Building it also turned up and fixed a JDBC driver bug and two harness
   measurement bugs ([09](09-implementation-notes.md) § 11b–11d).
2. ~~**A backpressure run.**~~ Done 2026-10-23: the sink stalled at every checkpoint, backpressure
   0.95–0.98 upstream, output exact ([05](05-evaluation.md) § Throughput and latency).
3. ~~**Run the non-stationary scenarios through the cluster.**~~ Done 2026-10-23: 4/4 exact, clean
   and with three crash modes ([05](05-evaluation.md) § Non-stationary traffic).
4. **Containers**, once a container runtime is available: same stack, same suite. Needs
   arm64-compatible images (SPEC.md).
5. **Cloud variant (optional)**: only with a spending cap, a resource inventory and a teardown
   script (SPEC.md, PLAN.md M6), and without assuming the managed service keeps the local
   guarantees.
6. **More than one partition**: needs the semantics change sketched in [02](02-semantics.md)
   first, then a new reference model.

## Cut line

Items 1–3 are worth doing before calling the correctness claim finished. Items 4–6 are
extensions. The project's thesis doesn't depend on them.
