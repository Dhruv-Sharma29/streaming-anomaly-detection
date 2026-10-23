"""Load, latency and backpressure measurements (milestone 6). Needs the cluster to itself.

    python -m eval.load capacity --rate-per-service 500 --duration-s 300
    python -m eval.load latency --speed 10 --label flush-on-checkpoint

capacity  Pre-loads the whole scenario into Kafka, then starts the job: measures how fast the
          pipeline drains a backlog (accepted events/s until every window is final and exact),
          sampling Flink's backpressure ratio while it does.
latency   Replays at `speed`x real time. Per window: latency = row first written - wall time
          the record that closed it (first record with event time >= window end + bound) was
          sent. Reported p50/p95/max.
"""

from __future__ import annotations

import argparse
import json
import statistics
import threading
import time
from datetime import UTC, datetime
from pathlib import Path

import httpx
import psycopg
from confluent_kafka import Producer

from detector.ewma import run as reference_alerts
from eval.run_pipeline import cancel, check_ordering_chain, clean_slate, submit, wait_running, wait_settled
from generator.generate import Config, generate
from generator.produce import produce
from infra import stack
from reference.compute import compute
from semantics import BOUND_MS, WINDOW_MS, parse

ROOT = Path(__file__).resolve().parents[1]


def backpressure_sampler(job_id: str, stop: threading.Event, samples: list) -> None:
    while not stop.is_set():
        try:
            for v in httpx.get(f"{stack.FLINK_URL}/jobs/{job_id}", timeout=5).json()["vertices"]:
                bp = httpx.get(f"{stack.FLINK_URL}/jobs/{job_id}/vertices/{v['id']}/backpressure", timeout=5).json()
                subtasks = bp.get("subtasks", [])
                if subtasks:
                    samples.append({"vertex": v["name"].split(" ->")[0][:30],
                                    "backpressured": max(s.get("ratio", 0) for s in subtasks),
                                    "busy": max(s.get("busyRatio", 0) for s in subtasks),
                                    "idle": min(s.get("idleRatio", 1) for s in subtasks)})
        except (httpx.HTTPError, KeyError, ValueError):
            pass
        stop.wait(2)


def capacity(rate_per_service: float, duration_s: int, seed: int) -> dict:
    meta, records = generate(Config(seed=seed, duration_s=duration_s, rate_per_service=rate_per_service))
    expected = compute([r["value"] for r in records])
    run_id = f"{datetime.now(UTC):%Y%m%dT%H%M%SZ}-capacity"
    topic = f"events-{run_id}"
    stack.create_topic(topic, partitions=1)
    stack.truncate_tables()
    clean_slate()
    produced = produce(records, topic)  # whole backlog first
    t_submit = time.monotonic()
    job_id = submit(topic, f"pipeline-{run_id}")
    samples: list = []
    stop = threading.Event()
    try:
        sampler = threading.Thread(target=backpressure_sampler, args=(job_id, stop, samples), daemon=True)
        sampler.start()
        wait_running(job_id)
        t_running = time.monotonic()
        check_ordering_chain(job_id)
        _, cmp, _ = wait_settled(expected, reference_alerts(expected["windows"]), timeout_s=900)
        t_done = time.monotonic()
    finally:
        stop.set()
        cancel(job_id)
    # The source starts consuming before every task reports RUNNING, so "from running" over-states
    # throughput and "from submit" (includes job start-up) under-states it. Both are reported.
    from_submit, from_running = t_done - t_submit, t_done - t_running
    vertices: dict[str, dict] = {}
    for s in samples:
        v = vertices.setdefault(s["vertex"], {"max_backpressured": 0.0, "max_busy": 0.0, "samples": 0})
        v["max_backpressured"] = round(max(v["max_backpressured"], s["backpressured"]), 2)
        v["max_busy"] = round(max(v["max_busy"], s["busy"]), 2)
        v["samples"] += 1
    return {"run_id": run_id, "mode": "capacity", "records": len(records), "accepted": expected["counts"]["accepted"],
            "produce": produced, "drain_from_submit_s": round(from_submit, 1), "drain_from_running_s": round(from_running, 1),
            "records_per_s_lower": round(len(records) / from_submit), "records_per_s_upper": round(len(records) / from_running),
            "exact": cmp["exact"], "vertices": vertices}


def latency(speed: float, seed: int, duration_s: int, label: str, sink_flush_ms: int) -> dict:
    meta, records = generate(Config(seed=seed, duration_s=duration_s))
    expected = compute([r["value"] for r in records])
    run_id = f"{datetime.now(UTC):%Y%m%dT%H%M%SZ}-latency-{label}"
    topic = f"events-{run_id}"
    stack.create_topic(topic, partitions=1)
    stack.truncate_tables()
    clean_slate()
    job_id = submit(topic, f"pipeline-{run_id}", sink_flush_ms=sink_flush_ms)
    try:
        wait_running(job_id)
        check_ordering_chain(job_id)
        producer = Producer({"bootstrap.servers": stack.BOOTSTRAP, "enable.idempotence": True, "acks": "all", "linger.ms": 0})
        start_ms = meta["start_ms"]
        wall0 = time.time()
        sent_at: list[float] = []
        for rec in records:  # pace by arrival time, compressed by `speed`
            due = wall0 + (rec["arrival_ms"] - start_ms) / 1000 / speed
            delay = due - time.time()
            if delay > 0:
                time.sleep(delay)
            producer.produce(topic, key=rec["key"], value=rec["value"], **({"partition": rec["partition"]} if "partition" in rec else {}))
            producer.poll(0)
            sent_at.append(time.time())
        producer.flush(30)
        _, cmp, _ = wait_settled(expected, reference_alerts(expected["windows"]), timeout_s=300)
    finally:
        cancel(job_id)
    # wall time at which each window became closable: first record pushing max event time past end + bound
    max_ts, closed_at = None, {}
    window_ends = sorted({w["window_start_ms"] + WINDOW_MS for w in expected["windows"]})
    idx = 0
    for rec, t in zip(records, sent_at):
        event, _ = parse(rec["value"])
        if not event:
            continue
        max_ts = event["ts_ms"] if max_ts is None else max(max_ts, event["ts_ms"])
        while idx < len(window_ends) and max_ts - BOUND_MS - 1 >= window_ends[idx] - 1:
            closed_at[window_ends[idx] - WINDOW_MS] = t
            idx += 1
    with psycopg.connect(stack.DSN, autocommit=True) as conn:
        rows = conn.execute("SELECT window_start_ms, extract(epoch FROM first_written_at) FROM windows").fetchall()
    lat = sorted((float(written) - closed_at[ws]) * 1000 for ws, written in rows if ws in closed_at)
    return {"run_id": run_id, "mode": "latency", "label": label, "speed": speed, "sink_flush_ms": sink_flush_ms,
            "exact": cmp["exact"], "windows": len(lat),
            "latency_ms_p50": round(statistics.median(lat)), "latency_ms_p95": round(lat[int(0.95 * (len(lat) - 1))]),
            "latency_ms_max": round(lat[-1])}


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("mode", choices=["capacity", "latency"])
    p.add_argument("--seed", type=int, default=21)
    p.add_argument("--rate-per-service", type=float, default=500)
    p.add_argument("--duration-s", type=int, default=300)
    p.add_argument("--speed", type=float, default=10)
    p.add_argument("--label", default="")
    p.add_argument("--sink-flush-ms", type=int, default=0, help="0 = flush only before checkpoints / on full batch")
    args = p.parse_args()
    stack.up()
    result = capacity(args.rate_per_service, args.duration_s, args.seed) if args.mode == "capacity" \
        else latency(args.speed, args.seed, args.duration_s, args.label, args.sink_flush_ms)
    out = ROOT / "results/raw/load_runs.jsonl"
    with open(out, "a") as f:
        f.write(json.dumps(result) + "\n")
    print(json.dumps(result, indent=1))


if __name__ == "__main__":
    main()
