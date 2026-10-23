"""EWMA z-score detector over **finalized** windows (after the allowed lateness has passed). Pure
function of the final window sequence per service, so the Flink implementation
(job/.../EwmaDetector.java) must produce identical alerts: same constants, same floating-point
operations in the same order.

Why finalized rather than first firings: a window's first firing depends on how far the watermark
lagged when late events arrived, which depends on replay speed and batching, so first-firing
alerts are not reproducible across runs or after crash recovery (observed: the first end-to-end
run at ~46x real time matched every final window but produced different alert scores). Final
values are deterministic. The cost is `allowed_lateness` (30 s) of extra detection delay.

Per service and metric: mean/variance by EWMA (alpha), updated during warm-up and on normal
windows only (an anomaly isn't absorbed into its own baseline). A window is anomalous if any
metric's z = (x - mean) / max(sqrt(var), floor) exceeds z_threshold (upward only). An alert opens
after `open_after` consecutive anomalous windows and closes after `close_after` normal ones.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field

from semantics import C, ERROR_STATUS_MIN  # noqa: F401  (ERROR_STATUS_MIN documents error_rate's basis)

D = C["detector"]
METRICS = ("latency_p95_ms", "error_rate", "queue_depth_max")


def features(row: dict) -> dict[str, float]:
    return {"latency_p95_ms": float(row["latency_p95_ms"]),
            "error_rate": row["error_count"] / row["request_count"],
            "queue_depth_max": float(row["queue_depth_max"])}


@dataclass
class ServiceState:
    n: int = 0
    mean: dict = field(default_factory=dict)
    var: dict = field(default_factory=dict)
    anomalous_streak: int = 0
    normal_streak: int = 0
    episode_start_ms: int | None = None
    open_alert: dict | None = None


def _update(st: ServiceState, x: dict[str, float]) -> None:
    for m in METRICS:
        if st.n == 0:
            st.mean[m], st.var[m] = x[m], 0.0
        else:
            d = x[m] - st.mean[m]
            st.mean[m] = st.mean[m] + D["alpha"] * d
            st.var[m] = (1.0 - D["alpha"]) * (st.var[m] + D["alpha"] * d * d)
    st.n += 1


def _z(st: ServiceState, m: str, x: float) -> float:
    floor = max(D["std_floor"][m]["abs"], D["std_floor"][m]["rel"] * st.mean[m])
    return (x - st.mean[m]) / max(math.sqrt(st.var[m]), floor)


def step(st: ServiceState, service: str, row: dict, z_threshold: float | None = None) -> list[dict]:
    """Process one finalized window; return alert records to upsert (open and/or close).
    `z_threshold` overrides the shared constant for sensitivity sweeps only; the default is what
    the Flink job uses (the Java/Python parity test runs with the default)."""
    z_thr = D["z_threshold"] if z_threshold is None else z_threshold
    x = features(row)
    ws = row["window_start_ms"]
    if st.n < D["warmup_windows"]:
        _update(st, x)
        return []
    z = {m: _z(st, m, x[m]) for m in METRICS}
    hot = sorted(m for m in METRICS if z[m] > z_thr)
    out = []
    if hot:
        st.anomalous_streak += 1
        st.normal_streak = 0
        if st.anomalous_streak == 1:
            st.episode_start_ms = ws
        if st.open_alert is None and st.anomalous_streak >= D["open_after"]:
            st.open_alert = {"alert_id": f"{service}:{st.episode_start_ms}", "service": service,
                             "episode_start_ms": st.episode_start_ms, "opened_window_ms": ws,
                             "closed_window_ms": None, "metrics": ",".join(hot), "z_at_open": max(z.values())}
            out.append(dict(st.open_alert))
    else:
        st.normal_streak += 1
        st.anomalous_streak = 0
        _update(st, x)
        if st.open_alert is not None and st.normal_streak >= D["close_after"]:
            st.open_alert["closed_window_ms"] = ws
            out.append(dict(st.open_alert))
            st.open_alert = None
    return out


def run(windows: list[dict], z_threshold: float | None = None) -> list[dict]:
    """Final state of every alert, keyed by alert_id (what the sink table should contain)."""
    states: dict[str, ServiceState] = {}
    alerts: dict[str, dict] = {}
    for row in sorted(windows, key=lambda r: (r["service"], r["window_start_ms"])):
        st = states.setdefault(row["service"], ServiceState())
        for a in step(st, row["service"], row, z_threshold):
            alerts[a["alert_id"]] = a
    return sorted(alerts.values(), key=lambda a: a["alert_id"])
