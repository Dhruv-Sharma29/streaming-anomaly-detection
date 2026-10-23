"""Reference calculator: the expected pipeline output, computed offline from a scenario file.

Uses only the raw record values in arrival order (never the generator's `kind` annotations), so it
derives every decision from the documented semantics, the same information the job has.

    python -m reference.compute .run/scenarios/dev-1.jsonl --out .run/expected/dev-1.json

Refuses to produce output if any watermark-dependent decision falls within the decision margin,
because Flink's lagging watermark could then legitimately decide differently (see compute()).
"""

from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from pathlib import Path

from generator.generate import read
from semantics import (DEDUP_HORIZON_MS, ERROR_STATUS_MIN, FLUSH_SERVICE, MARGIN_MS, late_slack_ms, p95, parse,
                       watermark, window_start)


class AmbiguousDecision(RuntimeError):
    pass


def _metrics(service: str, start: int, events: list[dict]) -> dict:
    return {"service": service, "window_start_ms": start, "request_count": len(events),
            "error_count": sum(1 for e in events if e["status"] >= ERROR_STATUS_MIN),
            "latency_p95_ms": p95([e["latency_ms"] for e in events]),
            "queue_depth_max": max(e["queue_depth"] for e in events)}


def compute(values: list[str]) -> dict:
    """Expected final windows (also the detector's input), drops, and counts.

    Margin checks are one-sided. Flink's watermark at any element is <= this reference watermark
    (periodic emission, per-partition minimum, records and watermarks in order per channel), so
    only decisions the reference makes *because the watermark has passed something* (late-drop,
    dedup state expired) can differ. Those must clear MARGIN_MS, which must exceed Flink's
    watermark lag in event time; that lag grows with replay speed (docs/02-semantics.md).
    """
    max_ts: int | None = None
    first_seen: dict[str, int] = {}
    final: dict[tuple[str, int], list[dict]] = defaultdict(list)
    dlq: Counter = Counter()
    late: list[dict] = []
    duplicates = flush = 0
    ambiguous: list[str] = []

    for raw in values:
        event, reason = parse(raw)
        if reason:
            dlq[reason] += 1
            continue
        ts = event["ts_ms"]
        wm = watermark(max_ts) if max_ts is not None else None  # watermark before this element
        max_ts = ts if max_ts is None else max(max_ts, ts)
        eid = event["event_id"]
        if eid in first_seen:
            expires = first_seen[eid] + DEDUP_HORIZON_MS
            if wm is None or wm < expires:
                duplicates += 1
                continue
            if wm - expires < MARGIN_MS:
                ambiguous.append(f"dedup-expiry {eid}")
        first_seen[eid] = ts
        if event["service"] == FLUSH_SERVICE:
            flush += 1
            continue
        key = (event["service"], window_start(ts))
        if wm is not None:
            slack = late_slack_ms(ts, wm)
            if slack <= 0:
                if slack > -MARGIN_MS:
                    ambiguous.append(f"late-drop {eid} slack={slack}")
                late.append({"event_id": eid, "service": event["service"], "window_start_ms": key[1]})
                continue
        final[key].append(event)

    if ambiguous:
        raise AmbiguousDecision(f"{len(ambiguous)} decisions within margin, e.g. {ambiguous[:3]}")

    windows = [_metrics(s, w, evs) for (s, w), evs in sorted(final.items())]
    accepted = sum(r["request_count"] for r in windows)
    return {"windows": windows, "late_dropped": late, "dlq_reasons": dict(dlq),
            "counts": {"records": len(values), "accepted": accepted, "dlq": sum(dlq.values()),
                       "duplicates_dropped": duplicates, "late_dropped": len(late), "flush": flush}}


def compute_file(path: Path) -> dict:
    _, records = read(path)
    return compute([r["value"] for r in records])


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("scenario", type=Path)
    p.add_argument("--out", type=Path, required=True)
    args = p.parse_args()
    result = compute_file(args.scenario)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(result))
    print(json.dumps(result["counts"]), f"windows={len(result['windows'])}", json.dumps(result["dlq_reasons"]))


if __name__ == "__main__":
    main()
