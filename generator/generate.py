"""Deterministic event generator with ground-truth anomaly labels and delivery faults.

Writes a scenario file (JSONL): a `meta` line (seed, config, anomaly labels), then the records in
**arrival order**, exactly as the producer will send them to Kafka. The reference calculator and
the producer both read this file, so replaying it reproduces the same input byte for byte.

    python -m generator.generate --seed 1 --duration-s 600 --out .run/scenarios/dev-1.jsonl

Delivery faults (rates are per valid event):
  mild disorder  every event arrives 0-3 s after its event time
  late           1%   arrive 25-32 s late  -> after the window's first firing, still applied
  very late      0.3% arrive 75-110 s late -> late-dropped, recorded
  duplicates     2%   re-delivered 0.1-3 s after the original
  malformed      0.1% truncated JSON        -> dead-letter
  schema v3      0.1% unsupported version   -> dead-letter
  schema v2      5%   v1 + optional region  -> accepted
Delay classes keep every include/drop decision >= decision_margin_ms away from the boundary
(docs/02-semantics.md); `check_margins` verifies it on the generated output.
"""

from __future__ import annotations

import argparse
import json
import math
import random
from dataclasses import asdict, dataclass, field
from pathlib import Path

from semantics import FLUSH_SERVICE

START_MS = 1_767_225_600_000  # 2026-01-01T00:00:00Z, fixed so runs are comparable
SERVICES = {"checkout": 45.0, "search": 30.0, "payments": 60.0, "inventory": 25.0}  # median latency ms
ANOMALY_KINDS = ["latency_spike", "error_burst", "outage", "latency_drift", "queue_growth"]


@dataclass(frozen=True)
class Interval:
    service: str
    kind: str          # an ANOMALY_KINDS entry, or "traffic_shift" (benign, not an anomaly)
    start_ms: int
    end_ms: int
    anomaly: bool
    severity: float = 1.0  # 0 = barely different from normal, 1 = obvious; scales the effect size


# Effect size at severity 0 and 1 (linear in between). Low severities are meant to be hard to
# detect, so recall and false-alert trade-offs are measurable rather than trivially perfect.
# The first version's low end (1.3x latency, +2% errors, ...) was still easy: a static threshold
# reached 99% held-out recall, so the evaluation could not separate detectors. Widened downward.
EFFECTS = {
    "latency_spike": (1.1, 6.0),     # latency multiplier
    "error_burst": (0.005, 0.30),    # extra 5xx probability
    "outage": (0.03, 0.85),          # fraction of requests failing slowly
    "latency_drift": (0.15, 4.0),    # extra latency multiplier reached at the end
    "queue_growth": (4, 600),        # queue depth added by the end
}


def effect(iv: Interval) -> float:
    lo, hi = EFFECTS[iv.kind]
    return lo + (hi - lo) * iv.severity


@dataclass
class Config:
    seed: int
    duration_s: int = 600
    rate_per_service: float = 20.0
    partitions: int = 1             # see docs/02-semantics.md: single partition for deterministic lateness
    warmup_s: int = 120              # no anomalies before this: detectors need a baseline
    n_anomalies: int = 5
    n_benign: int = 2
    late_rate: float = 0.01
    very_late_rate: float = 0.003
    duplicate_rate: float = 0.02
    malformed_rate: float = 0.001
    schema_v3_rate: float = 0.001
    schema_v2_rate: float = 0.05
    services: dict = field(default_factory=lambda: dict(SERVICES))
    # Non-stationary baselines (off by default, so existing scenarios stay byte-identical): a slow
    # "daily" wave in latency and benign permanent level shifts (a deploy that makes a service
    # slower for good). Driven by a separate RNG stream so turning it on doesn't reshuffle the rest.
    nonstationary: bool = False
    wave_amplitude: float = 0.25
    wave_period_s: int = 300
    n_level_shifts: int = 2


def baseline_factor(cfg: Config, service: str, t_ms: int, shifts: list[Interval], phases: dict) -> float:
    """Multiplier on a service's normal latency at time t (1.0 when stationary)."""
    if not cfg.nonstationary:
        return 1.0
    seconds = (t_ms - START_MS) / 1000
    f = 1.0 + cfg.wave_amplitude * math.sin(2 * math.pi * seconds / cfg.wave_period_s + phases[service])
    for iv in shifts:
        if iv.service == service and t_ms >= iv.start_ms:
            f *= iv.severity  # for level shifts, severity holds the permanent multiplier
    return f


def nonstationary_parts(cfg: Config) -> tuple[list[Interval], dict]:
    rng = random.Random(cfg.seed * 7_654_321 + 17)
    phases = {s: rng.uniform(0, 2 * math.pi) for s in sorted(cfg.services)}
    shifts = []
    if cfg.nonstationary:
        end = START_MS + cfg.duration_s * 1000
        for _ in range(cfg.n_level_shifts):
            start = START_MS + rng.randint(cfg.warmup_s, cfg.duration_s - 120) * 1000
            start -= (start - START_MS) % 10_000
            shifts.append(Interval(rng.choice(sorted(cfg.services)), "level_shift", start, end, False,
                                   round(rng.uniform(1.3, 1.6), 3)))
    return shifts, phases


def schedule(cfg: Config, rng: random.Random) -> list[Interval]:
    """Non-overlapping (per service) anomaly and benign intervals after the warm-up."""
    intervals: list[Interval] = []
    busy: dict[str, list[tuple[int, int]]] = {s: [] for s in cfg.services}
    end_limit = cfg.duration_s * 1000 - 60_000
    if end_limit - cfg.warmup_s * 1000 < 90_000 + 120_000:
        raise ValueError(f"duration_s={cfg.duration_s} too short for warm-up {cfg.warmup_s}s plus anomalies; use >= 400")

    def place(kind: str, anomaly: bool) -> None:
        for _ in range(200):
            service = rng.choice(sorted(cfg.services))
            length = rng.randint(30, 90) * 1000
            start = rng.randint(cfg.warmup_s * 1000, end_limit - length)
            start -= start % 10_000  # align to windows so labels map cleanly onto windows
            span = (start - 60_000, start + length + 60_000)  # keep a recovery gap between intervals
            if all(span[1] <= a or span[0] >= b for a, b in busy[service]):
                busy[service].append((start, start + length))
                severity = round(rng.uniform(0.0, 1.0), 3) if anomaly else 1.0
                intervals.append(Interval(service, kind, START_MS + start, START_MS + start + length, anomaly, severity))
                return
        raise RuntimeError("could not place interval; scenario too crowded")

    for i in range(cfg.n_anomalies):
        place(ANOMALY_KINDS[i % len(ANOMALY_KINDS)], True)
    for _ in range(cfg.n_benign):
        place("traffic_shift", False)
    return sorted(intervals, key=lambda iv: (iv.start_ms, iv.service))


def _active(intervals: list[Interval], service: str, ts: int) -> list[Interval]:
    return [iv for iv in intervals if iv.service == service and iv.start_ms <= ts < iv.end_ms]


def events_for(cfg: Config, rng: random.Random, intervals: list[Interval]) -> list[dict]:
    events = []
    end = START_MS + cfg.duration_s * 1000
    shifts, phases = nonstationary_parts(cfg)
    for service, median in sorted(cfg.services.items()):
        ts, seq = float(START_MS), 0
        while True:
            rate = cfg.rate_per_service * (2.0 if any(iv.kind == "traffic_shift" for iv in _active(intervals, service, int(ts))) else 1.0)
            ts += rng.expovariate(rate) * 1000
            if ts >= end:
                break
            t = int(ts)
            latency = rng.lognormvariate(math.log(median), 0.35) * baseline_factor(cfg, service, t, shifts, phases)
            status = 500 if rng.random() < 0.005 else 200
            queue = 10 + rng.randint(0, 6)
            for iv in _active(intervals, service, t):
                if not iv.anomaly:
                    continue
                progress = (t - iv.start_ms) / (iv.end_ms - iv.start_ms)
                size = effect(iv)
                if iv.kind == "latency_spike":
                    latency *= size
                elif iv.kind == "error_burst" and rng.random() < size:
                    status = 500
                elif iv.kind == "outage":
                    status, latency = (503, rng.uniform(1500, 3000)) if rng.random() < size else (status, latency)
                elif iv.kind == "latency_drift":
                    latency *= 1 + size * progress
                elif iv.kind == "queue_growth":
                    queue += int(size * progress)
            seq += 1
            events.append({"schema_version": 1, "event_id": f"{service[:3]}-{seq:09d}", "service": service,
                           "ts_ms": t, "latency_ms": round(latency, 1), "status": status, "queue_depth": queue})
    return events


# Late delay classes only apply to events at least this far before the end. Near the end, normal
# traffic stops advancing the watermark, so a delayed event's include/drop decision could land
# inside the decision margin (found by test_generated_scenarios_are_unambiguous_and_reconcile).
LATE_CUTOFF_MS = 130_000


def deliveries(cfg: Config, rng: random.Random, events: list[dict]) -> list[dict]:
    """Assign arrival times, duplicates, malformed and schema-variant records; sort by arrival."""
    out = []
    late_cutoff = START_MS + cfg.duration_s * 1000 - LATE_CUTOFF_MS
    for order, e in enumerate(events):
        r = rng.random() if e["ts_ms"] < late_cutoff else 1.0
        if r < cfg.very_late_rate:
            delay, kind = rng.uniform(75_000, 110_000), "very_late"
        elif r < cfg.very_late_rate + cfg.late_rate:
            delay, kind = rng.uniform(25_000, 32_000), "late"
        else:
            delay, kind = rng.uniform(0, 3_000), "normal"
        arrival = e["ts_ms"] + delay
        if rng.random() < cfg.schema_v2_rate:
            e = {**e, "schema_version": 2, "region": rng.choice(["eu-west", "us-east", "ap-south"])}
        value = json.dumps(e, separators=(",", ":"))
        # Kafka key = event_id: spreads load over all partitions (keying by 4 services could leave a
        # partition empty and stall the watermark) and keeps duplicates on the same partition.
        out.append({"arrival_ms": arrival, "order": order, "key": e["event_id"], "value": value, "kind": kind})
        if rng.random() < cfg.duplicate_rate:
            out.append({"arrival_ms": arrival + rng.uniform(100, 3_000), "order": order, "key": e["event_id"],
                        "value": value, "kind": "duplicate"})
        r = rng.random()
        if r < cfg.malformed_rate:
            out.append({"arrival_ms": arrival + 1, "order": order, "key": e["event_id"],
                        "value": value[: len(value) // 2], "kind": "malformed"})
        elif r < cfg.malformed_rate + cfg.schema_v3_rate:
            bad = json.dumps({**e, "schema_version": 3, "event_id": e["event_id"] + "-v3"}, separators=(",", ":"))
            out.append({"arrival_ms": arrival + 1, "order": order, "key": e["event_id"] + "-v3", "value": bad, "kind": "schema_v3"})
    out.sort(key=lambda d: (d["arrival_ms"], d["order"]))
    # End-of-test markers: one per partition, far enough ahead that every window fires and every
    # lateness period expires. The job treats this service as watermark-only control traffic.
    flush_ts = max(e["ts_ms"] for e in events) + 120_000
    for p in range(cfg.partitions):
        marker = {"schema_version": 1, "event_id": f"flush-{p}", "service": FLUSH_SERVICE, "ts_ms": flush_ts,
                  "latency_ms": 0, "status": 0, "queue_depth": 0}
        out.append({"arrival_ms": flush_ts, "order": -1, "key": FLUSH_SERVICE, "partition": p,
                    "value": json.dumps(marker, separators=(",", ":")), "kind": "flush"})
    return out


def generate(cfg: Config) -> tuple[dict, list[dict]]:
    rng = random.Random(cfg.seed)
    intervals = schedule(cfg, rng)
    events = events_for(cfg, rng, intervals)
    records = deliveries(cfg, rng, events)
    shifts, _ = nonstationary_parts(cfg)
    meta = {"type": "meta", "config": asdict(cfg), "start_ms": START_MS,
            "labels": [asdict(iv) for iv in intervals + shifts], "n_events": len(events), "n_records": len(records)}
    return meta, records


def write(path: Path, meta: dict, records: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        f.write(json.dumps(meta) + "\n")
        for i, r in enumerate(records):
            rec = {"type": "record", "seq": i, "key": r["key"], "value": r["value"], "kind": r["kind"],
                   "arrival_ms": round(r["arrival_ms"])}
            if "partition" in r:
                rec["partition"] = r["partition"]
            f.write(json.dumps(rec) + "\n")


def read(path: Path) -> tuple[dict, list[dict]]:
    lines = [json.loads(line) for line in Path(path).read_text().splitlines()]
    return lines[0], lines[1:]


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--seed", type=int, required=True)
    p.add_argument("--duration-s", type=int, default=600)
    p.add_argument("--rate", type=float, default=20.0, help="events/s per service")
    p.add_argument("--partitions", type=int, default=1)
    p.add_argument("--out", type=Path, required=True)
    args = p.parse_args()
    cfg = Config(seed=args.seed, duration_s=args.duration_s, rate_per_service=args.rate, partitions=args.partitions)
    meta, records = generate(cfg)
    write(args.out, meta, records)
    print(f"{args.out}: {meta['n_events']} events, {meta['n_records']} records, "
          f"{sum(1 for l in meta['labels'] if l['anomaly'])} anomalies")


if __name__ == "__main__":
    main()
