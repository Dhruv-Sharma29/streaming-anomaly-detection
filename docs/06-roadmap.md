# 06 — Roadmap

PLAN.md is the original execution plan. This doc records how each milestone closed, with the
evidence, and what is left. Status as of 2026-10-23. The last recorded runs are from 2026-10-06.

## Milestones

| Milestone | Exit criterion (PLAN.md) | State | Evidence |
| --- | --- | --- | --- |
| M1 Reference calculation | Expected results unambiguous for every seed | **Done** | `reference/compute.py` raises `AmbiguousDecision`; `tests/test_reference.py` over 20 seeds |
| M2 Streaming baseline | Output equals reference for in-order and out-of-order streams | **Done** | `results/raw/development_runs.jsonl` → `e2e-single-partition` exact; clean runs 01–05 |
| M3 Failure correctness | After every crash scenario, final output equals reference | **Done, with a gap** | 16/16 exact ([05](05-evaluation.md)). Crashes are injected with the job idle, not before/after a sink write or mid-checkpoint ([03](03-recovery.md)) |
| M4 Anomaly baseline | EWMA + threshold, alert dedup, P/R/delay/false alerts | **Done** | `results/summary/detection.json`; parity test; open/close rule |
| M5 Learned detector (optional) | Separate training runs, same alert budget | **Done, offline** | `results/summary/detection_budget.json`. Not deployed in the job |
| M6 Load + optional cloud | Throughput, latency, recovery; backpressure visible and bounded | **Local part done**; cloud not done | `results/raw/load_runs*.jsonl`. No backpressure occurred, so "bounded" is untested |

### PLAN.md test list

| Test | State |
| --- | --- |
| Replayed ids don't inflate counts in-horizon | Covered: dedup + exact windows in every run |
| Late events follow the documented rule | Covered: `late_match` + boundary unit tests |
| Crash before/after sink write matches reference | **Partly**: crashes at input fractions with the job idle, not targeted at sink writes |
| Backpressure bounded, no silent drop | **Partly**: no drop (exact under a 2.5 M-record backlog); no backpressure observed |
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

1. **Crash during active processing.** Drop the 8 s pause for a new crash mode: kill at a random
   offset while producing, and kill on checkpoint start. This closes the M3 gap and is the
   cheapest change with the largest effect on what the correctness claim covers ([07](07-risks.md) § R2).
2. **A backpressure run.** Throttle the sink (or slow PostgreSQL) until the ordered chain backs up,
   and check the run is still exact and that the sampler sees a non-zero ratio. This closes M6's
   "visible and bounded".
3. **Run the non-stationary scenarios through the cluster** at least once, so the offline
   detector numbers rest on an observed exact match for both regimes ([04](04-detection.md)).
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
