"""Semantics, reference calculator, and generator tests (milestone 1: no broker needed)."""

from __future__ import annotations

import json

import pytest

from generator.generate import Config, generate
from reference.compute import AmbiguousDecision, compute
from semantics import BOUND_MS, LATENESS_MS, WINDOW_MS, late_slack_ms, p95, parse

T0 = 1_767_225_600_000


def ev(eid, ts, service="svc", latency=10.0, status=200, queue=1, version=1, **extra):
    return json.dumps({"schema_version": version, "event_id": eid, "service": service, "ts_ms": ts,
                       "latency_ms": latency, "status": status, "queue_depth": queue, **extra})


# --- parsing ------------------------------------------------------------------------------------

@pytest.mark.parametrize("raw,reason", [
    ('{"schema_version": 1, "event_id": ', "malformed_json"),
    ("[1, 2]", "not_an_object"),
    (ev("a", T0, version=3), "unsupported_schema_version"),
    (ev("a", T0, version=True), "unsupported_schema_version"),
    (json.dumps({"schema_version": 1, "event_id": "a"}), "bad_field:service"),
    (ev("a", T0, status=True), "bad_field:status"),          # bool is not an int here
    (ev("a", T0, latency=-1.0), "invalid_value"),
    (ev("a", T0, service=""), "invalid_value"),
])
def test_invalid_records_get_a_reason(raw, reason):
    assert parse(raw) == (None, reason)


def test_v1_and_v2_are_accepted():
    assert parse(ev("a", T0))[1] is None
    assert parse(ev("a", T0, version=2, region="eu-west"))[1] is None


def test_p95_nearest_rank():
    assert p95(list(range(1, 101))) == 95
    assert p95([7.0]) == 7.0


# --- lateness boundary --------------------------------------------------------------------------

def test_late_slack_matches_flink_is_window_late():
    ts = T0 + 3_000                     # window [T0, T0+10s), maxTimestamp = T0 + 9_999
    boundary = T0 + WINDOW_MS - 1 + LATENESS_MS
    assert late_slack_ms(ts, boundary) == 0          # watermark == max + lateness -> late
    assert late_slack_ms(ts, boundary - 1) == 1      # one ms earlier -> still applied


# --- reference on hand-built streams ------------------------------------------------------------

def test_windows_duplicates_late_and_dlq():
    values = [
        ev("a", T0 + 1_000, latency=10, status=200, queue=3),
        ev("b", T0 + 2_000, latency=30, status=503, queue=9),
        ev("a", T0 + 1_000, latency=10, status=200, queue=3),   # duplicate re-delivery
        '{"broken',                                             # dead-letter
        ev("c", T0 + 15_000, latency=20),                        # next window
        ev("x", T0 + 80_000),                                    # advances watermark to T0+75s-1ms
        ev("d", T0 + 4_000, latency=99),                         # 76 s behind: late-dropped
    ]
    out = compute(values)
    rows = {r["window_start_ms"]: r for r in out["windows"]}
    assert rows[T0] == {"service": "svc", "window_start_ms": T0, "request_count": 2, "error_count": 1,
                        "latency_p95_ms": 30, "queue_depth_max": 9}
    assert rows[T0 + 10_000]["request_count"] == 1
    assert out["counts"] == {"records": 7, "accepted": 4, "dlq": 1, "duplicates_dropped": 1,
                             "late_dropped": 1, "flush": 0}
    assert out["late_dropped"] == [{"event_id": "d", "service": "svc", "window_start_ms": T0}]


def test_late_event_within_lateness_is_applied():
    # Watermark T0+25s-1ms: window [T0, T0+10s) has fired (>= 5 s margin) but lateness hasn't expired.
    values = [ev("a", T0 + 1_000), ev("x", T0 + 30_000), ev("late", T0 + 2_000)]
    out = compute(values)
    assert next(r for r in out["windows"] if r["window_start_ms"] == T0)["request_count"] == 2


def test_drop_decisions_too_close_to_the_boundary_are_refused():
    # Watermark = window max + lateness + 10 s (< 15 s margin): reference drops, a lagging Flink
    # watermark might not have passed the boundary yet.
    values = [ev("x", T0 + 9_999 + 30_000 + 10_000 + BOUND_MS + 1), ev("edge", T0 + 1_000)]
    with pytest.raises(AmbiguousDecision, match="late-drop"):
        compute(values)



def test_applied_side_needs_no_margin():
    # Slack of 1 ms: the reference applies it, and Flink's watermark can only be lower, so it agrees.
    values = [ev("a0", T0 + 500), ev("x", T0 + 9_999 + 30_000 - 1 + BOUND_MS + 1), ev("a", T0 + 1_000)]
    out = compute(values)
    assert out["counts"]["late_dropped"] == 0 and out["windows"][0]["request_count"] == 2


def test_flush_markers_advance_time_but_create_no_windows():
    values = [ev("a", T0 + 1_000), ev("flush-0", T0 + 500_000, service="__flush__")]
    out = compute(values)
    assert [r["service"] for r in out["windows"]] == ["svc"] and out["counts"]["flush"] == 1


# --- generator ----------------------------------------------------------------------------------

def test_generator_is_deterministic():
    assert generate(Config(seed=3, duration_s=400)) == generate(Config(seed=3, duration_s=400))
    assert generate(Config(seed=3, duration_s=400)) != generate(Config(seed=4, duration_s=400))
    with pytest.raises(ValueError, match="too short"):
        generate(Config(seed=3, duration_s=200))


@pytest.mark.parametrize("seed", range(1, 21))
def test_generated_scenarios_are_unambiguous_and_reconcile(seed):
    meta, records = generate(Config(seed=seed))
    out = compute([r["value"] for r in records])  # raises if any decision is within the margin
    kinds = {}
    for r in records:
        kinds[r["kind"]] = kinds.get(r["kind"], 0) + 1
    assert out["counts"]["duplicates_dropped"] == kinds.get("duplicate", 0)
    assert out["counts"]["dlq"] == kinds.get("malformed", 0) + kinds.get("schema_v3", 0)
    assert out["counts"]["accepted"] + out["counts"]["late_dropped"] == meta["n_events"]


def test_labels_respect_warmup_and_do_not_overlap_per_service():
    meta, _ = generate(Config(seed=11))
    labels = meta["labels"]
    assert sum(l["anomaly"] for l in labels) == 5
    for l in labels:
        assert l["start_ms"] >= T0 + 120_000 and l["start_ms"] % WINDOW_MS == 0
    for a in labels:
        for b in labels:
            if a is not b and a["service"] == b["service"]:
                assert a["end_ms"] <= b["start_ms"] or b["end_ms"] <= a["start_ms"]
