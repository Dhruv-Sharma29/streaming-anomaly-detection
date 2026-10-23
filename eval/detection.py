"""Detector quality, reported separately from pipeline correctness.

    python -m eval.detection --out results/summary/detection

Detectors run on reference final windows. The correctness suite shows the Flink pipeline produces
exactly these windows and alerts, so this measures the pipeline's alerts without 30 cluster runs.

  ewma    detector/ewma.py (also in the Flink job). Parameters fixed a priori, not tuned.
  static  per-service thresholds learned from each run's warm-up windows:
          p95 > a * median warm-up p95, error_rate > b, queue_max > c * median warm-up queue;
          (a, b, c) chosen on dev seeds 1-20 by best F1, then frozen. Same open/close rule as ewma.

Evaluated on held-out seeds 101-130. Matching: an alert matches an anomaly label on the same service
if it opens within [label start, label end + 30 s]. Unmatched alerts are false alerts (those during
a benign traffic shift are counted separately). Delay = end of the opening window - label start
(decision delay, event time). Emission adds the 30 s allowed lateness (detector runs on final windows).
"""

from __future__ import annotations

import argparse
import itertools
import json
import statistics
from collections import defaultdict
from pathlib import Path

from detector.ewma import run as ewma_run
from generator.generate import Config, generate
from reference.compute import compute
from semantics import C, LATENESS_MS, WINDOW_MS

D = C["detector"]
DEV_SEEDS = range(1, 21)
HELDOUT_SEEDS = range(101, 131)
MATCH_GRACE_MS = 30_000


def static_run(windows: list[dict], a: float, b: float, c: float) -> list[dict]:
    alerts: dict[str, dict] = {}
    by_service = defaultdict(list)
    for w in sorted(windows, key=lambda r: (r["service"], r["window_start_ms"])):
        by_service[w["service"]].append(w)
    for service, rows in by_service.items():
        warm = rows[: D["warmup_windows"]]
        p95_ref = statistics.median(r["latency_p95_ms"] for r in warm)
        q_ref = statistics.median(r["queue_depth_max"] for r in warm)
        streak_a = streak_n = 0
        episode = None
        open_alert = None
        for r in rows[D["warmup_windows"]:]:
            hot = (r["latency_p95_ms"] > a * p95_ref or r["error_count"] / r["request_count"] > b
                   or r["queue_depth_max"] > c * q_ref)
            if hot:
                streak_a, streak_n = streak_a + 1, 0
                if streak_a == 1:
                    episode = r["window_start_ms"]
                if open_alert is None and streak_a >= D["open_after"]:
                    open_alert = {"alert_id": f"{service}:{episode}", "service": service,
                                  "opened_window_ms": r["window_start_ms"]}
                    alerts[open_alert["alert_id"]] = open_alert
            else:
                streak_n, streak_a = streak_n + 1, 0
                if open_alert is not None and streak_n >= D["close_after"]:
                    open_alert = None
    return list(alerts.values())


def score(alerts: list[dict], labels: list[dict]) -> dict:
    anomalies = [l for l in labels if l["anomaly"]]
    benign = [l for l in labels if not l["anomaly"]]
    detected: dict[int, int] = {}  # anomaly index -> delay ms
    false_alerts = false_in_benign = 0
    for a in alerts:
        t = a["opened_window_ms"]
        hits = [i for i, l in enumerate(anomalies)
                if l["service"] == a["service"] and l["start_ms"] <= t <= l["end_ms"] + MATCH_GRACE_MS]
        if hits:
            i = hits[0]
            delay = t + WINDOW_MS - anomalies[i]["start_ms"]
            detected[i] = min(delay, detected.get(i, delay))
        else:
            false_alerts += 1
            if any(b["service"] == a["service"] and b["start_ms"] <= t <= b["end_ms"] + MATCH_GRACE_MS for b in benign):
                false_in_benign += 1
    return {"anomalies": [{**l, "detected": i in detected, "delay_ms": detected.get(i)} for i, l in enumerate(anomalies)],
            "alerts": len(alerts), "false_alerts": false_alerts, "false_in_benign": false_in_benign}


def evaluate(seeds, detect) -> dict:
    per_anomaly: list[dict] = []
    alerts = false = benign_false = 0
    for seed in seeds:
        meta, records = generate(Config(seed=seed))
        windows = compute([r["value"] for r in records])["windows"]
        s = score(detect(windows), meta["labels"])
        per_anomaly += s["anomalies"]
        alerts += s["alerts"]
        false += s["false_alerts"]
        benign_false += s["false_in_benign"]
    tp = sum(a["detected"] for a in per_anomaly)
    precision = (alerts - false) / alerts if alerts else 0.0
    recall = tp / len(per_anomaly)
    delays = [a["delay_ms"] / 1000 for a in per_anomaly if a["detected"]]
    by_kind = defaultdict(lambda: [0, 0])
    by_sev = defaultdict(lambda: [0, 0])
    for a in per_anomaly:
        by_kind[a["kind"]][0] += a["detected"]
        by_kind[a["kind"]][1] += 1
        bucket = f"{int(a['severity'] * 5) / 5:.1f}-{int(a['severity'] * 5) / 5 + 0.2:.1f}"
        by_sev[bucket][0] += a["detected"]
        by_sev[bucket][1] += 1
    return {"runs": len(list(seeds)), "anomalies": len(per_anomaly), "alerts": alerts, "detected": tp,
            "precision": round(precision, 3), "recall": round(recall, 3),
            "f1": round(2 * precision * recall / (precision + recall), 3) if precision + recall else 0.0,
            "false_alerts": false, "false_alerts_in_benign_shift": benign_false,
            "delay_s_median": statistics.median(delays) if delays else None,
            "delay_s_p90": sorted(delays)[int(0.9 * (len(delays) - 1))] if delays else None,
            "recall_by_kind": {k: f"{d}/{n}" for k, (d, n) in sorted(by_kind.items())},
            "recall_by_severity": {k: f"{d}/{n}" for k, (d, n) in sorted(by_sev.items())}}


def tune_static() -> tuple[tuple[float, float, float], dict]:
    grid = itertools.product([1.5, 2.0, 3.0], [0.02, 0.05, 0.10], [1.5, 2.0, 3.0])
    best = None
    for params in grid:
        res = evaluate(DEV_SEEDS, lambda w, p=params: static_run(w, *p))
        if best is None or res["f1"] > best[1]["f1"]:
            best = (params, res)
    return best


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--out", type=Path)
    args = p.parse_args()
    (a, b, c), static_dev = tune_static()
    results = {
        "ewma_dev": evaluate(DEV_SEEDS, ewma_run),
        "ewma_heldout": evaluate(HELDOUT_SEEDS, ewma_run),
        "static_params": {"latency_x": a, "error_rate": b, "queue_x": c},
        "static_dev": static_dev,
        "static_heldout": evaluate(HELDOUT_SEEDS, lambda w: static_run(w, a, b, c)),
        "emission_extra_delay_s": LATENESS_MS / 1000,
    }
    print(json.dumps(results, indent=1))
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.with_suffix(".json").write_text(json.dumps(results, indent=1))


if __name__ == "__main__":
    main()
