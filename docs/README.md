# Streaming anomaly detection with crash recovery — Documentation

A Kafka → Flink → PostgreSQL pipeline that computes per-service window metrics and anomaly alerts,
and is checked against an independent offline reference **exactly** through duplicate,
out-of-order, late and malformed events, worker kills, job restores and database outages.

These docs are the plan and the design record. Read them in order the first time and use them as
reference afterwards.

## Reading order

| # | Doc | What it settles |
|---|---|---|
| 00 | [Charter](00-charter.md) | Problem, scope in/out with reasons, success criteria and which are met |
| 01 | [Architecture](01-architecture.md) | The pipeline and the oracle, three design rules, checkpointed state, tables, repo layout |
| 02 | [Stream semantics](02-semantics.md) | Event time, watermark, lateness, dedup, the guarantee claimed and not claimed |
| 03 | [Recovery](03-recovery.md) | Why at-least-once + deterministic upserts gives exact final state; the crash harness and its blind spots |
| 04 | [Detection](04-detection.md) | EWMA on finalized windows, Java/Python parity, static and learned comparators, scoring protocol |
| 05 | [Evaluation](05-evaluation.md) | Every result: 16 suite runs, recovery times, detector tables with intervals, throughput and latency |
| 06 | [Roadmap](06-roadmap.md) | How each milestone closed, the evidence, what is next and in what order |
| 07 | [Risks](07-risks.md) | Thesis vs delivery risks, claim hygiene, "is this exactly-once?" |
| 08 | [Learning checklist](08-learning-checklist.md) | Skills per milestone and interview talking points mapped to the code |
| 09 | [Implementation notes](09-implementation-notes.md) | Where the code departs from the plan and the docs, and why |

Outside `docs/`: [start.md](../start.md) (run what needs no daemons), [RUNBOOK.md](../RUNBOOK.md)
(the Kafka/Flink/PostgreSQL steps), [SPEC.md](../SPEC.md) and [PLAN.md](../PLAN.md) (the original
brief and plan).

## The one-paragraph version

Streaming demos usually show a dashboard filling up and stop there. This project starts from the
opposite end: **a streaming result is only interesting if you can say exactly what it should have
been, and show the job produced that after it was killed.** That forces three things most demos
skip. First, every decision (lateness, de-duplication, window contents, alerts) is made a function
of the input *sequence*, not of timing, so a few hundred lines of Python can compute the expected
tables. Second, lateness is decided in a checkpointed operator, because Flink does not checkpoint
watermarks, and the test that proved it is an adversarial restore where the first record after
recovery is very late. Third, the claim is stated narrowly: at-least-once input plus idempotent
upserts of deterministic values gives exact *final* tables, not exactly-once side effects.
Detection is evaluated separately, under one false-alert budget, on held-out runs.

## Status

**As of 2026-10-23, milestones M1–M6 are done locally. The last runs were recorded 2026-10-06.**
16/16 failure-injection runs match the reference exactly, and `pytest` passes 52 tests (re-run
2026-10-23). Not done: the optional cloud variant, containers, and the learned detector inside the
job. Two gaps matter more than those. Crashes are injected while the job is idle, so replay of
in-flight records is barely tested, and no run ever produced backpressure. See
[06-roadmap.md](06-roadmap.md) for what's next, and [09-implementation-notes.md](09-implementation-notes.md)
for three doc corrections, seven bugs found by the exact comparison and the harness, and the honest
downgrades that followed.

## Conventions used in these docs

- **MUST / SHOULD / MAY** are load-bearing where they appear. MUST items are acceptance criteria.
- Anything marked **[verify]** is a claim not confirmed against a primary source or a recorded run.
  Confirm it before relying on it.
- Every number cites the file it comes from, usually under `results/`. If a number in another
  document disagrees with the cited file, the file wins.
- Constants are written once, in `semantics/constants.json`. The docs quote them and do not
  redefine them.
