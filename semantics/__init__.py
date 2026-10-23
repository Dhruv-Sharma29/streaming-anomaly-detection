"""Single source of truth for stream semantics (docs/semantics.md). The Flink job reads the same
constants.json, so Python reference and Java job can't drift apart silently."""

from __future__ import annotations

import json
import math
from pathlib import Path

CONSTANTS_FILE = Path(__file__).with_name("constants.json")
C = json.loads(CONSTANTS_FILE.read_text())

WINDOW_MS: int = C["window_ms"]
BOUND_MS: int = C["watermark_bound_ms"]
LATENESS_MS: int = C["allowed_lateness_ms"]
DEDUP_HORIZON_MS: int = C["dedup_horizon_ms"]
MARGIN_MS: int = C["decision_margin_ms"]
ERROR_STATUS_MIN: int = C["error_status_min"]
ACCEPTED_VERSIONS: frozenset[int] = frozenset(C["accepted_schema_versions"])
FLUSH_SERVICE: str = C["flush_service"]

REQUIRED = {"schema_version": int, "event_id": str, "service": str, "ts_ms": int,
            "latency_ms": (int, float), "status": int, "queue_depth": int}


def window_start(ts_ms: int) -> int:
    return ts_ms - ts_ms % WINDOW_MS


def watermark(max_ts_ms: int) -> int:
    """Flink BoundedOutOfOrdernessWatermarks: max timestamp seen - bound - 1 ms."""
    return max_ts_ms - BOUND_MS - 1


def window_max_ts(ts_ms: int) -> int:
    return window_start(ts_ms) + WINDOW_MS - 1


def late_slack_ms(ts_ms: int, watermark_ms: int) -> int:
    """> 0: the event is still applied to its window; <= 0: late-dropped (Flink's isWindowLate:
    window.maxTimestamp() + allowedLateness <= currentWatermark)."""
    return window_max_ts(ts_ms) + LATENESS_MS - watermark_ms


def parse(raw: str) -> tuple[dict | None, str | None]:
    """Validate one raw record. Returns (event, None) or (None, dead-letter reason)."""
    try:
        event = json.loads(raw)
    except ValueError:
        return None, "malformed_json"
    if not isinstance(event, dict):
        return None, "not_an_object"
    version = event.get("schema_version")
    if type(version) is not int or version not in ACCEPTED_VERSIONS:
        return None, "unsupported_schema_version"
    for field, typ in REQUIRED.items():
        value = event.get(field)
        if isinstance(value, bool) or not isinstance(value, typ):
            return None, f"bad_field:{field}"
    if not event["service"] or event["latency_ms"] < 0 or event["queue_depth"] < 0 \
            or not math.isfinite(event["latency_ms"]):
        return None, "invalid_value"
    return event, None


def p95(values: list[float]) -> float:
    ordered = sorted(values)
    return ordered[max(0, math.ceil(0.95 * len(ordered)) - 1)]
