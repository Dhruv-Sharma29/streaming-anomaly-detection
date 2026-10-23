# Start here

How to get this project to a result, and what to look at while it runs. The
[README](README.md) argues for the design, and [docs/](docs/README.md) is the design record.

Steps 1–4 need **no running services**. The semantics, the reference, the generator and both
detector implementations are real work you can check without Kafka or Flink. Everything that
starts a daemon (PostgreSQL, Kafka, Flink) is in [RUNBOOK.md](RUNBOOK.md).

---

## 1. Install

Requirements: Linux on aarch64 for the bundled toolchain (the JDK download and the PostgreSQL
library path are aarch64; [docs/07-risks.md](docs/07-risks.md) § D1), Python 3.12 and
[`uv`](https://docs.astral.sh/uv/).

```bash
scripts/setup_tools.sh      # JDK 17, Kafka 4.3.1, Flink 2.2.1, Maven 3.9.16, PostgreSQL 16 into .tools/
uv sync                     # Python deps into .venv/ (pyproject.toml, uv.lock)
(cd job && JAVA_HOME=../.tools/jdk ../.tools/maven/bin/mvn -B -Dmaven.repo.local=../.tools/m2 package)
```

`setup_tools.sh` downloads about 2 GB, verifies every archive against the publisher's checksum,
installs nothing system-wide, and is idempotent. `rm -rf .tools` undoes it. The Maven build
produces `job/target/stream-job-0.1.0.jar`, which the parity test and the cluster runs both use.

macOS (Apple silicon) has a separate path, untested on a real Mac: see README § macOS.

---

## 2. Run the tests

```bash
uv run pytest
```

Expect `52 passed` (re-run 2026-10-23, about 20 s). They cover parsing and dead-letter reasons,
the late-window boundary against Flink's rule, the reference on hand-built streams, generator
determinism and reconciliation over 20 seeds, Java ↔ Python detector parity on 10 seeds, and
learned-feature causality.

The 10 parity tests are **skipped** if the jar or the JDK is missing. Build step 1's jar first,
or you are not testing the Java detector.

---

## 3. See the oracle work, before any cluster is involved

Generate one scenario and compute what the pipeline *should* write (output below re-run
2026-10-23):

```bash
uv run python -m generator.generate --seed 1 --out .run/scenarios/dev-1.jsonl
uv run python -m reference.compute .run/scenarios/dev-1.jsonl --out .run/expected/dev-1.json
```

```
.run/scenarios/dev-1.jsonl: 50499 events, 51584 records, 5 anomalies
{"records": 51584, "accepted": 50383, "dlq": 90, "duplicates_dropped": 994, "late_dropped": 116, "flush": 1} windows=240 {"malformed_json": 42, "unsupported_schema_version": 48}
```

What to read in it:
- **51,584 records** is the same count as `results/suite-logs/01.log` (seed 1, clean run). The
  cluster run consumed exactly this file.
- `accepted + late_dropped = events` (50,383 + 116 = 50,499). Every valid event is either in a
  window or in `late_events`. Nothing disappears.
- `duplicates_dropped` equals the number of duplicates the generator injected (tested for 20 seeds).
- If the reference ever prints `AmbiguousDecision`, a decision fell within 15 s of a boundary where
  Flink could legitimately decide differently. It refuses rather than guesses
  ([docs/02-semantics.md](docs/02-semantics.md) § Decision margin).

The first line of the scenario file is a `meta` record with the anomaly labels. The rest are
records in arrival order, each with a `kind` (`normal`, `late`, `very_late`, `duplicate`,
`malformed`, `schema_v3`, `flush`). The reference never reads `kind`, only the raw values.

---

## 4. Detector quality, offline (CPU only)

```bash
uv run python -m eval.detection --out results/summary/detection     # M4: EWMA vs static, stationary
uv run python -m eval.detection_budget                              # M5: three detectors, one budget, two regimes
```

Both evaluate on reference windows, generated in-process. No cluster is needed. **Both overwrite
the committed files in `results/summary/`.** Run them on a scratch copy, or check `git diff` after,
if you want to compare with the published numbers. `detection_budget` uses a 10-process pool and
trains gradient-boosted models (runtime not recorded in the repo). The published results and how
to read them are in [docs/05-evaluation.md](docs/05-evaluation.md) § Detection quality.

---

## 5. Run the real pipeline

Needs PostgreSQL, Kafka and Flink running locally. Follow [RUNBOOK.md](RUNBOOK.md): start the
stack, one clean run, one crash run, the 16-run suite (~10 min), the dashboard demo, and the load
measurements.

---

## 6. Read a run's output

Each pipeline run prints, and `eval/suite.sh` saves to `results/suite-logs/NN.log`, a record like:

| Field | Meaning |
| --- | --- |
| `windows_expected` / `windows_got` / `_missing` / `_extra` / `_mismatched` | Row-by-row comparison of `windows` with the reference |
| `late_match` | The set of late-dropped `event_id`s equals the reference's |
| `dlq_match` | Dead-letter counts per reason equal the reference's |
| `alerts_match` | Alerts equal the Python detector's, including `z_at_open` |
| `window_operator_late` | Tripwire: events Flink's own window operator dropped. Must be 0 |
| `window_writes` | Total upserts into `windows`, more than 240 because late events re-upsert rows |
| `crash_info.recovery_s` | Failure → every task RUNNING again ([docs/03-recovery.md](docs/03-recovery.md) § Recovery time) |
| `exact` | All of the above hold |

A non-exact run also prints `mismatch examples`: expected and actual rows side by side.

---

## 7. Where to read next

- [docs/02-semantics.md](docs/02-semantics.md): the rules both sides implement.
- [docs/03-recovery.md](docs/03-recovery.md): why the final state is exact, and what the crash
  suite doesn't test yet.
- [docs/09-implementation-notes.md](docs/09-implementation-notes.md): the bugs the exact comparison
  found.

---

## Troubleshooting

| Symptom | Cause / fix |
| --- | --- |
| Parity tests skipped | No `job/target/stream-job-0.1.0.jar` or no `.tools/jdk` (Linux) / `$JAVA_HOME` (macOS). Build the jar (step 1) |
| `setup_tools.sh`: `CHECKSUM MISMATCH` | The download was corrupted or replaced. The file is deleted; re-run |
| `setup_tools.sh` fails at the JDK on x86-64 | The script pins an aarch64 JDK. See [docs/07-risks.md](docs/07-risks.md) § D1 |
| `AmbiguousDecision` from the reference | A custom scenario put a decision within the 15 s margin. Use the generator's default delay classes |
| `ValueError: duration_s=… too short` | The generator needs at least 400 s for warm-up plus anomalies |
| Port already in use (19xxx) | Another stack is running. Set `STACK_PORT_BASE` (see `infra/stack.py`) |
