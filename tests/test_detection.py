"""Learned-detector feature causality, and stability of the stationary generator."""

from __future__ import annotations

import copy
import hashlib
import json

from eval.detection_budget import window_features
from generator.generate import Config, generate

# Stationary seed-1 scenario as verified by the correctness suite (51,584 records). The optional
# non-stationary mode must not change stationary scenarios.
SEED1_SHA256 = "eeeceec8808ffa3ffedf6c229d814c62a51900cb186de1b821d579b1f888cb08"


def rows(n: int) -> list[dict]:
    return [{"service": "s", "window_start_ms": i * 10_000, "request_count": 200, "error_count": i % 3,
             "latency_p95_ms": 40.0 + i, "queue_depth_max": 12} for i in range(n)]


def test_features_use_only_past_windows():
    base = rows(40)
    changed = copy.deepcopy(base)
    for r in changed[25:]:                      # rewrite the future
        r["latency_p95_ms"], r["error_count"] = 999.0, 150
    assert window_features(base)[:25] == window_features(changed)[:25]


def test_stationary_scenarios_unchanged_by_nonstationary_option():
    _, records = generate(Config(seed=1))
    assert hashlib.sha256(json.dumps(records).encode()).hexdigest() == SEED1_SHA256


def test_nonstationary_adds_benign_level_shifts():
    meta, _ = generate(Config(seed=1, nonstationary=True))
    shifts = [l for l in meta["labels"] if l["kind"] == "level_shift"]
    assert len(shifts) == 2 and not any(l["anomaly"] for l in shifts)


def test_process_state_without_proc_uses_ps(monkeypatch):
    """macOS has no /proc; exercise the ps fallback on Linux by hiding it."""
    import os
    import subprocess
    import sys
    import time

    from infra import stack
    real_isdir = os.path.isdir
    monkeypatch.setattr(stack.os.path, "isdir", lambda p: False if p == "/proc" else real_isdir(p))
    assert stack._proc_state(os.getpid()) in ("R", "S")
    child = subprocess.Popen([sys.executable, "-c", "pass"])
    time.sleep(0.5)
    assert stack._proc_state(child.pid) == "Z" and stack._alive(child.pid) is False
