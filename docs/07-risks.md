# 07 — Risks and failure modes

Two kinds: **thesis risks**, where the project works but shows less than it claims, and
**delivery risks**, where it doesn't run for someone else. The first kind matters more, because
"16/16 exact" can look complete while resting on easy cases.

## Thesis risks

### R1 — The oracle shares the job's assumptions *(medium likelihood, high impact)*

The reference is independent *code* (Python vs Java) but not an independent *specification*. Both
read `semantics/constants.json` and implement [02](02-semantics.md). Suppose that doc misdescribes
Flink, for example the exact late-window boundary or when timers fire relative to watermarks. Then
both sides can be wrong the same way, and the comparison passes.

**Detection:** the boundary is pinned against Flink's own rule in a unit test
(`test_late_slack_matches_flink_is_window_late` mirrors `isWindowLate`). The tripwire table
`window_operator_late` must stay empty in every run: if Flink's own window operator ever drops
something the filter forwarded, the two models of lateness disagree. The 15 s decision margin keeps
the generator away from boundaries where the two could legitimately differ.

**Mitigation:** the margin and the tripwire are already in place. What would strengthen it: a
small hand-computed scenario with expected rows written out by hand, not by the reference.

### R2 — Crash points are the easy ones *(certain, medium impact)*

The harness pauses 8 s before every injected failure, so the job is idle and the restore point
covers all prior input ([03](03-recovery.md) § What the suite does not exercise). The
replay-and-overwrite path, which carries most of the at-least-once argument, is barely exercised.

**Detection:** read the harness, not the result line. `run_once` in `eval/run_pipeline.py`.

**Mitigation:** the scope of the claim already says "final table contents" and "process failure".
The fix is a crash mode without the pause ([06](06-roadmap.md) item 1). Until then, the honest
summary is: *state restore is verified, including the adversarial watermark case; replay under
load is argued, not tested.*

### R3 — Exactness by construction *(medium likelihood, medium impact)*

The generator keeps every include/drop decision at least 15 s from a boundary (delay classes
0–3 s, 25–32 s, 75–110 s; no late classes in the last 130 s). Real traffic has no such gaps. The
reference would *refuse* such a scenario (`AmbiguousDecision`), not produce a wrong answer, so the
system fails loudly rather than silently. But the suite says nothing about inputs near the
boundary.

**Detection:** `tests/test_reference.py::test_drop_decisions_too_close_to_the_boundary_are_refused`.
**Mitigation:** stated in [02](02-semantics.md) § Decision margin. Anyone quoting the result should
quote the margin with it.

### R4 — Detector results over-read *(medium likelihood, medium impact)*

Three ways to over-claim. (a) The README row "EWMA z-score (runs in the Flink job)" is EWMA at
z = 3.5, while the job runs z = 4.0. (b) Every detector number is on synthetic data from one
generator, 150 held-out anomalies per regime. (c) The learned detector's advantage is specifically
fewer false alerts on the *benign non-stationarity this generator produces*.

**Detection:** `results/summary/detection_budget.json` → `detectors.ewma.knob`; bootstrap
intervals in the same file.
**Mitigation:** [05](05-evaluation.md) reports the deployed configuration (z = 4.0, M4) and the
re-tuned one (M5) separately, plus the negative M4 result: the tuned static rule beat untuned EWMA
on recall.

### R5 — Single-host, single-partition numbers quoted as capacity *(low likelihood, low impact)*

178k–208k records/s is a single ordered consumer thread draining a preloaded backlog on one
machine. It's a ceiling of this design, not a throughput claim for the system class.

**Mitigation:** [00](00-charter.md) lists throughput as a non-goal. [05](05-evaluation.md) says
which vertex is saturated.

## Delivery risks

### D1 — The toolchain is pinned to Linux on aarch64 *(high likelihood for x86 readers)*

`scripts/setup_tools.sh` downloads an **aarch64** Temurin JDK on Linux, and `infra/stack.py` uses an
`aarch64-linux-gnu` library path for the user-space PostgreSQL. README's "Requirements: Linux" is
broader than that. On x86-64 Linux the JDK step would need a different URL and checksum
**[verify]** (not tried).

### D2 — macOS path untested on a real Mac *(stated in README)*

The macOS branches (Homebrew JDK and PostgreSQL, `ps` instead of `/proc`, `gtimeout`) are exercised
on Linux only (`tests/test_detection.py::test_process_state_without_proc_uses_ps`). README says so.

### D3 — Version churn in the Flink ecosystem *(medium)*

Flink 2.2.1 with Kafka connector `5.0.0-2.2` (`job/pom.xml`). The JDBC connector's API changes are
one reason the sink is hand-written ([03](03-recovery.md)). A Flink upgrade could change watermark
or timer behaviour that [02](02-semantics.md) relies on. The suite would catch that as a mismatch,
not silently.

### D4 — Downloads and ports *(low)*

`setup_tools.sh` needs about 2 GB from `downloads.apache.org`, GitHub and the Ubuntu archive,
with checksums verified. The stack binds ports 19000+ on 127.0.0.1 (`STACK_PORT_BASE` changes the
base), chosen below the ephemeral range.

## Claim hygiene

Say:
- "16/16 failure-injection runs produce exactly the reference's final tables, including restores
  where the first record after recovery is very late."
- "At-least-once input plus idempotent upserts of deterministic values; final state is exact."
- "Recovery 3.2–10.0 s after a TaskManager kill, 8.3–8.5 s after a job restore, 8.4 s from the
  start of an 8 s database outage; one host, local processes."

Don't say:
- "Exactly-once." Not end to end, and not for side effects.
- "Tested under crashes during processing." Not yet (R2).
- "The in-job detector scores F1 0.973." That's z = 3.5; the job's z = 4.0 scores 0.966 (M4).
- "Handles backpressure." None was observed.

## The question to have a real answer for

> "Is this exactly-once?"

No, and it doesn't need to be for what it claims. Kafka offsets and operator state are restored
together from a checkpoint. The sink is at-least-once. Every write is an upsert of a value that is
a pure function of the input sequence, so a replayed write lands the same value. That is
"effectively once" *for the final contents of keyed tables*. It isn't for anything that observes
intermediate writes: a reader can see a row twice-written, or briefly stale, during recovery. Then
the follow-up worth volunteering: the crash suite so far kills an idle job, so the overwrite path
is argued more than measured. The next crash mode to add is the one that kills mid-stream.
