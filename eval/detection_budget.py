"""Detector comparison under a fixed false-alert budget, on stationary and non-stationary traffic.

    python -m eval.detection_budget

Detectors (same alert rule: open after 2 flagged windows, close after 3 normal ones):
  ewma     detector/ewma.py; knob = z threshold
  static   per-service thresholds from warm-up medians; knob = (latency x, error rate, queue x)
  learned  gradient-boosted classifier on causal window features (ratios to rolling medians,
           rolling z-scores, deltas); knob = probability threshold; trained on dev windows only

Protocol: for each detector, the knob maximising dev recall with at most BUDGET false alerts per
run is chosen on dev seeds 1-20 (both regimes pooled), then frozen. For the learned detector the knob
is chosen on out-of-fold predictions (trained on dev seeds 1-10, scored on 11-20 and vice versa):
a first version chose it on its own training data, where everything looks perfect. The final
model is then trained on all dev seeds. Held-out differences get 95% bootstrap intervals over scenarios. Results are on held-out seeds
101-130, per regime. Non-stationary traffic adds a slow "daily" latency wave and benign permanent
level shifts (alerts during them count as false alerts).

Windows come from the offline reference, which the correctness suite shows the Flink job reproduces
exactly. The learned detector is evaluated offline; it is not deployed in the Flink job.
"""

from __future__ import annotations

import itertools
import json
import math
import statistics
from collections import defaultdict
from multiprocessing import Pool
from pathlib import Path

import numpy as np
from sklearn.ensemble import HistGradientBoostingClassifier

from detector.ewma import run as ewma_run
from eval.detection import score, static_run
from generator.generate import Config, generate
from reference.compute import compute
from semantics import C

ROOT = Path(__file__).resolve().parents[1]
D = C["detector"]
DEV, HELDOUT = range(1, 21), range(101, 131)
REGIMES = {"stationary": False, "nonstationary": True}
BUDGET = 0.5  # max false alerts per 10-minute run, on dev
EPS = {"latency_p95_ms": 1.0, "error_rate": 0.005, "queue_depth_max": 1.0, "request_count": 1.0}


def scenario(args: tuple[int, bool]) -> dict:
    seed, nonstationary = args
    meta, records = generate(Config(seed=seed, nonstationary=nonstationary))
    return {"seed": seed, "nonstationary": nonstationary, "labels": meta["labels"],
            "windows": compute([r["value"] for r in records])["windows"]}


def by_service(windows: list[dict]) -> dict[str, list[dict]]:
    out = defaultdict(list)
    for w in sorted(windows, key=lambda r: (r["service"], r["window_start_ms"])):
        out[w["service"]].append(w)
    return out


def window_features(rows: list[dict]) -> list[list[float]]:
    """Causal features for each window of one service (uses only earlier windows)."""
    series = {"latency_p95_ms": [r["latency_p95_ms"] for r in rows],
              "error_rate": [r["error_count"] / r["request_count"] for r in rows],
              "queue_depth_max": [float(r["queue_depth_max"]) for r in rows],
              "request_count": [float(r["request_count"]) for r in rows]}
    feats = []
    for i in range(len(rows)):
        f = []
        for m, xs in series.items():
            x, eps = xs[i], EPS[m]
            short, hist = xs[max(0, i - 6):i] or [x], xs[max(0, i - 30):i] or [x]
            med_s, med_h = statistics.median(short), statistics.median(hist)
            mu, sd = statistics.fmean(hist), (statistics.pstdev(hist) if len(hist) > 1 else 0.0)
            prev = xs[i - 1] if i else x
            f += [x / (med_s + eps), x / (med_h + eps), (x - mu) / (sd + eps), (x - prev) / (med_h + eps)]
        f.append(series["error_rate"][i])
        feats.append(f)
    return feats


def window_labels(rows: list[dict], labels: list[dict]) -> list[int]:
    return [int(any(l["anomaly"] and l["service"] == r["service"] and l["start_ms"] <= r["window_start_ms"] < l["end_ms"]
                    for l in labels)) for r in rows]


def episodes(service: str, rows: list[dict], flags: list[bool]) -> list[dict]:
    alerts, a_streak, n_streak, episode, is_open = [], 0, 0, None, False
    for r, hot in zip(rows, flags):
        if hot:
            a_streak, n_streak = a_streak + 1, 0
            if a_streak == 1:
                episode = r["window_start_ms"]
            if not is_open and a_streak >= D["open_after"]:
                alerts.append({"alert_id": f"{service}:{episode}", "service": service, "opened_window_ms": r["window_start_ms"]})
                is_open = True
        else:
            n_streak, a_streak = n_streak + 1, 0
            if is_open and n_streak >= D["close_after"]:
                is_open = False
    return alerts


class OutOfFold:
    """Two models trained on disjoint halves of dev; each dev scenario is scored by the model that
    didn't see it (knob selection only)."""

    def __init__(self, scenarios: list[dict]):
        self.fold = {s["seed"]: s["seed"] % 2 for s in scenarios}
        self.models = {f: Learned([s for s in scenarios if self.fold[s["seed"]] != f]) for f in (0, 1)}
        self.by_windows = {id(s["windows"]): self.fold[s["seed"]] for s in scenarios}

    def __call__(self, windows: list[dict], threshold: float) -> list[dict]:
        return self.models[self.by_windows[id(windows)]](windows, threshold)


class Learned:
    def __init__(self, scenarios: list[dict]):
        X, y = [], []
        for s in scenarios:
            for rows in by_service(s["windows"]).values():
                X += window_features(rows)[D["warmup_windows"]:]
                y += window_labels(rows, s["labels"])[D["warmup_windows"]:]
        self.clf = HistGradientBoostingClassifier(max_iter=300, learning_rate=0.05, random_state=0).fit(np.array(X), np.array(y))
        self.n_train, self.pos_rate = len(y), float(np.mean(y))

    def __call__(self, windows: list[dict], threshold: float) -> list[dict]:
        alerts = []
        for service, rows in by_service(windows).items():
            p = self.clf.predict_proba(np.array(window_features(rows)))[:, 1]
            flags = [i >= D["warmup_windows"] and p[i] > threshold for i in range(len(rows))]
            alerts += episodes(service, rows, flags)
        return alerts


def evaluate(scenarios: list[dict], detect) -> dict:
    anomalies, alerts, false, benign = [], 0, 0, 0
    for s in scenarios:
        r = score(detect(s["windows"]), s["labels"])
        anomalies += r["anomalies"]
        alerts += r["alerts"]
        false += r["false_alerts"]
        benign += r["false_in_benign"]
    detected = sum(a["detected"] for a in anomalies)
    precision = (alerts - false) / alerts if alerts else 0.0
    recall = detected / len(anomalies)
    delays = sorted(a["delay_ms"] / 1000 for a in anomalies if a["detected"])
    return {"precision": round(precision, 3), "recall": round(recall, 3),
            "f1": round(2 * precision * recall / (precision + recall), 3) if precision + recall else 0.0,
            "false_alerts_per_run": round(false / len(scenarios), 2), "false_in_benign_per_run": round(benign / len(scenarios), 2),
            "delay_s_median": statistics.median(delays) if delays else None,
            "recall_low_severity": round(statistics.mean(a["detected"] for a in anomalies if a["severity"] < 0.2), 3)}


def bootstrap_diff(held: list[dict], det_a, det_b, n: int = 2000, seed: int = 0) -> dict:
    """95% bootstrap interval (resampling scenarios) for A minus B on recall and false alerts per run."""
    per = []
    for s in held:
        ra, rb = score(det_a(s["windows"]), s["labels"]), score(det_b(s["windows"]), s["labels"])
        per.append((sum(a["detected"] for a in ra["anomalies"]), sum(a["detected"] for a in rb["anomalies"]),
                    len(ra["anomalies"]), ra["false_alerts"], rb["false_alerts"]))
    rng = np.random.default_rng(seed)
    rec, fa = [], []
    for _ in range(n):
        idx = rng.integers(0, len(per), len(per))
        da, db, tot, fa_a, fa_b = (sum(per[i][j] for i in idx) for j in range(5))
        rec.append((da - db) / tot)
        fa.append((fa_a - fa_b) / len(idx))
    q = lambda xs: [round(float(np.percentile(xs, 2.5)), 3), round(float(np.percentile(xs, 97.5)), 3)]  # noqa: E731
    return {"recall_diff_ci95": q(rec), "false_alerts_per_run_diff_ci95": q(fa)}


def choose(dev: list[dict], detector, knobs) -> tuple:
    best = None
    for k in knobs:
        r = evaluate(dev, lambda w, k=k: detector(w, k))
        key = (r["false_alerts_per_run"] <= BUDGET, r["recall"] if r["false_alerts_per_run"] <= BUDGET else -r["false_alerts_per_run"])
        if best is None or key > best[0]:
            best = (key, k, r)
    return best[1], best[2]


def main() -> None:
    with Pool(10) as pool:
        dev = pool.map(scenario, [(s, ns) for s in DEV for ns in REGIMES.values()])
        held = pool.map(scenario, [(s, ns) for s in HELDOUT for ns in REGIMES.values()])
    learned = Learned(dev)
    oof = OutOfFold(dev)
    detectors = {
        "ewma": (lambda w, z: ewma_run(w, z_threshold=z), [3.0, 3.5, 4.0, 5.0, 6.0, 8.0, 10.0]),
        "static": (lambda w, k: static_run(w, *k), list(itertools.product([1.3, 1.5, 2.0, 3.0], [0.02, 0.05, 0.10], [1.5, 2.0, 3.0]))),
        "learned": (learned, [0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]),
    }
    report = {"budget_false_alerts_per_run": BUDGET, "learned_training_windows": learned.n_train,
              "learned_positive_rate": round(learned.pos_rate, 3), "detectors": {}}
    for name, (detect, knobs) in detectors.items():
        knob, dev_result = choose(dev, oof if name == "learned" else detect, knobs)
        report["detectors"][name] = {"knob": knob, "dev": dev_result, "heldout": {
            regime: evaluate([s for s in held if s["nonstationary"] == ns], lambda w, k=knob: detect(w, k))
            for regime, ns in REGIMES.items()}}
    chosen = {name: (lambda w, d=detectors[name][0], k=r["knob"]: d(w, k)) for name, r in report["detectors"].items()}
    report["learned_minus_baselines"] = {
        regime: {base: bootstrap_diff([s for s in held if s["nonstationary"] == ns], chosen["learned"], chosen[base])
                 for base in ("ewma", "static")} for regime, ns in REGIMES.items()}
    (ROOT / "results/summary/detection_budget.json").write_text(json.dumps(report, indent=1, default=str))
    print(f"budget: <= {BUDGET} false alerts/run on dev; learned trained on {learned.n_train} dev windows")
    for name, r in report["detectors"].items():
        print(f"{name:8} knob={r['knob']} dev recall {r['dev']['recall']} FA/run {r['dev']['false_alerts_per_run']}")
        for regime, h in r["heldout"].items():
            print(f"   {regime:14} P {h['precision']:.3f} R {h['recall']:.3f} F1 {h['f1']:.3f} FA/run {h['false_alerts_per_run']} "
                  f"(benign {h['false_in_benign_per_run']}) low-sev R {h['recall_low_severity']} delay {h['delay_s_median']}s")
    for regime, d in report["learned_minus_baselines"].items():
        print(f"learned minus baselines, {regime}: {d}")


if __name__ == "__main__":
    main()
