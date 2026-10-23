"""Replay a scenario file into Kafka in arrival order.

    python -m generator.produce .run/scenarios/dev-1.jsonl --topic events-run1 [--rate 2000]
                                [--from-seq 0] [--to-seq 30000]

Idempotent producer with acks=all, so a retried send can't create a duplicate on the broker:
duplicates in the stream are only the ones the scenario injects on purpose. Per-partition order is
preserved; flush markers go to their explicit partition. --from/--to let crash tests pause and
resume a replay at exact record positions.
"""

from __future__ import annotations

import argparse
import time

from confluent_kafka import Producer

from generator.generate import read
from infra.stack import BOOTSTRAP


def produce(records: list[dict], topic: str, rate: float = 0.0) -> dict:
    producer = Producer({"bootstrap.servers": BOOTSTRAP, "enable.idempotence": True, "acks": "all",
                         "linger.ms": 5, "compression.type": "lz4"})
    failures: list[str] = []

    def on_delivery(err, _msg) -> None:
        if err is not None:
            failures.append(str(err))

    t0 = time.monotonic()
    for i, rec in enumerate(records):
        kwargs = {"partition": rec["partition"]} if "partition" in rec else {}
        while True:
            try:
                producer.produce(topic, key=rec["key"], value=rec["value"], on_delivery=on_delivery, **kwargs)
                break
            except BufferError:
                producer.poll(0.05)
        producer.poll(0)
        if rate > 0:
            ahead = (i + 1) / rate - (time.monotonic() - t0)
            if ahead > 0:
                time.sleep(ahead)
    remaining = producer.flush(60)
    elapsed = time.monotonic() - t0
    if failures or remaining:
        raise RuntimeError(f"{len(failures)} delivery failures, {remaining} unflushed: {failures[:3]}")
    return {"records": len(records), "seconds": round(elapsed, 2), "records_per_s": round(len(records) / elapsed, 1)}


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("scenario")
    p.add_argument("--topic", required=True)
    p.add_argument("--rate", type=float, default=0.0, help="records/s; 0 = as fast as possible")
    p.add_argument("--from-seq", type=int, default=0)
    p.add_argument("--to-seq", type=int, default=None, help="exclusive")
    args = p.parse_args()
    _, records = read(args.scenario)
    print(produce(records[args.from_seq:args.to_seq], args.topic, args.rate))


if __name__ == "__main__":
    main()
