"""End-to-end run: scenario -> Kafka -> Flink -> Postgres, compared with the reference.

    python -m eval.run_pipeline --seed 1                                  # clean run
    python -m eval.run_pipeline --seed 2 --crash taskmanager --at 0.4     # SIGKILL TM at 40% of input
    python -m eval.run_pipeline --seed 3 --crash job --at 0.5             # cancel job, resume from checkpoint
    python -m eval.run_pipeline --seed 4 --crash postgres --at 0.5        # sink database restarts

Each run uses a fresh topic and consumer group. Comparison is exact: final windows, first-firing
alerts, late-dropped event ids, dead-letter counts by reason. One JSON line per run goes to
results/raw/.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path

import httpx
import psycopg

from detector.ewma import run as reference_alerts
from generator.generate import Config, generate
from generator.produce import produce
from infra import stack
from reference.compute import compute
from semantics import CONSTANTS_FILE

ROOT = Path(__file__).resolve().parents[1]
JAR = ROOT / "job/target/stream-job-0.1.0.jar"
FLINK = ROOT / ".tools/flink/bin/flink"


# --- Flink job control -------------------------------------------------------------------------

def _flink_env() -> dict:
    import os
    return {**os.environ, "JAVA_HOME": str(stack.JAVA_HOME), "FLINK_CONF_DIR": str(stack.flink_conf_dir())}


def submit(topic: str, group: str, from_checkpoint: str | None = None, sink_flush_ms: int = 0) -> str:
    cmd = [str(FLINK), "run", "-d"]
    if from_checkpoint:
        cmd += ["-s", from_checkpoint]
    cmd += ["-c", "pipeline.StreamingJob", str(JAR), "--bootstrap", stack.BOOTSTRAP, "--topic", topic,
            "--group", group, "--jdbc-url", stack.JDBC_URL, "--constants", str(CONSTANTS_FILE),
            "--checkpoint-ms", "3000", "--sink-flush-ms", str(sink_flush_ms), "--name", topic]
    out = subprocess.run(cmd, env=_flink_env(), capture_output=True, text=True, timeout=120)
    for line in out.stdout.splitlines():
        if "JobID" in line:
            return line.split()[-1]
    raise RuntimeError(f"submit failed:\n{out.stdout}\n{out.stderr}")


def clean_slate(min_free_slots: int = 4, timeout_s: float = 120) -> None:
    """Cancel jobs left over from earlier runs and wait until enough slots are free. A run that
    once raised before cancelling leaked a job that later held slots (results/suite.log, seed 4)."""
    for j in httpx.get(f"{stack.FLINK_URL}/jobs/overview", timeout=5).json()["jobs"]:
        if j["state"] not in ("CANCELED", "FAILED", "FINISHED"):
            cancel(j["jid"])
    t0 = time.monotonic()
    while time.monotonic() - t0 < timeout_s:
        overview = httpx.get(f"{stack.FLINK_URL}/overview", timeout=5).json()
        if overview["slots-available"] >= min_free_slots:
            return
        time.sleep(1)
    raise RuntimeError("Flink slots not released")


def job_state(job_id: str) -> str:
    return httpx.get(f"{stack.FLINK_URL}/jobs/{job_id}", timeout=5).json()["state"]


def wait_running(job_id: str, timeout_s: float = 120) -> float:
    """Wait until every task of the job is RUNNING; returns seconds waited."""
    t0 = time.monotonic()
    while time.monotonic() - t0 < timeout_s:
        try:
            info = httpx.get(f"{stack.FLINK_URL}/jobs/{job_id}", timeout=5).json()
            if info["state"] == "RUNNING" and all(v["status"] == "RUNNING" for v in info["vertices"]):
                return time.monotonic() - t0
        except (httpx.HTTPError, KeyError):
            pass
        time.sleep(0.5)
    raise RuntimeError(f"job {job_id} not running after {timeout_s}s")


def check_ordering_chain(job_id: str) -> None:
    """Source -> validate -> lateness must be one chained task at parallelism 1, or arrival order
    (which every lateness decision depends on) is not preserved."""
    vertices = httpx.get(f"{stack.FLINK_URL}/jobs/{job_id}", timeout=5).json()["vertices"]
    chain = [v for v in vertices if "lateness" in v["name"]]
    if len(chain) != 1 or chain[0]["parallelism"] != 1 or "kafka-events" not in chain[0]["name"] \
            or "validate" not in chain[0]["name"]:
        raise RuntimeError(f"ordering chain broken: {[(v['name'], v['parallelism']) for v in vertices]}")


def num_restarts(job_id: str) -> int:
    metrics = httpx.get(f"{stack.FLINK_URL}/jobs/{job_id}/metrics?get=numRestarts", timeout=5).json()
    return int(float(metrics[0]["value"])) if metrics else 0


def latest_checkpoint(job_id: str) -> str | None:
    info = httpx.get(f"{stack.FLINK_URL}/jobs/{job_id}/checkpoints", timeout=5).json()
    completed = (info.get("latest") or {}).get("completed")
    return completed["external_path"] if completed else None


def wait_checkpoint(job_id: str, timeout_s: float = 60) -> str:
    t0 = time.monotonic()
    while time.monotonic() - t0 < timeout_s:
        path = latest_checkpoint(job_id)
        if path:
            return path
        time.sleep(0.5)
    raise RuntimeError("no completed checkpoint")


def cancel(job_id: str) -> None:
    try:
        httpx.patch(f"{stack.FLINK_URL}/jobs/{job_id}?mode=cancel", timeout=10)
        for _ in range(60):
            if job_state(job_id) in ("CANCELED", "FAILED", "FINISHED"):
                return
            time.sleep(0.5)
    except httpx.HTTPError:
        pass


# --- results -----------------------------------------------------------------------------------

def read_results() -> dict:
    with psycopg.connect(stack.DSN, autocommit=True) as conn:
        windows = [dict(zip(("service", "window_start_ms", "request_count", "error_count", "latency_p95_ms", "queue_depth_max"), r))
                   for r in conn.execute("SELECT service, window_start_ms, request_count, error_count, latency_p95_ms, "
                                         "queue_depth_max FROM windows ORDER BY service, window_start_ms")]
        alerts = [dict(zip(("alert_id", "service", "episode_start_ms", "opened_window_ms", "closed_window_ms", "metrics", "z_at_open"), r))
                  for r in conn.execute("SELECT alert_id, service, episode_start_ms, opened_window_ms, closed_window_ms, "
                                        "metrics, z_at_open FROM alerts ORDER BY alert_id")]
        late = {r[0] for r in conn.execute("SELECT event_id FROM late_events")}
        tripwire = conn.execute("SELECT count(*) FROM window_operator_late").fetchone()[0]
        dlq = dict(conn.execute("SELECT reason, count(*) FROM dlq GROUP BY reason").fetchall())
        writes = conn.execute("SELECT coalesce(sum(writes), 0), count(*) FROM windows").fetchone()
    return {"windows": windows, "alerts": alerts, "late": late, "dlq": dlq, "window_writes": int(writes[0]),
            "window_operator_late": tripwire}


def compare(expected: dict, expected_alerts: list[dict], got: dict) -> dict:
    exp_w = {(w["service"], w["window_start_ms"]): w for w in expected["windows"]}
    got_w = {(w["service"], w["window_start_ms"]): w for w in got["windows"]}
    mismatched = [k for k in exp_w if k in got_w and exp_w[k] != got_w[k]]
    return {
        "windows_expected": len(exp_w), "windows_got": len(got_w),
        "windows_missing": len(exp_w.keys() - got_w.keys()), "windows_extra": len(got_w.keys() - exp_w.keys()),
        "windows_mismatched": len(mismatched), "mismatch_examples": [[exp_w[k], got_w[k]] for k in mismatched[:2]],
        "late_match": got["late"] == {e["event_id"] for e in expected["late_dropped"]},
        "dlq_match": got["dlq"] == expected["dlq_reasons"],
        "alerts_match": got["alerts"] == expected_alerts,
        "alerts_expected": len(expected_alerts), "alerts_got": len(got["alerts"]),
        "window_operator_late": got["window_operator_late"],
    }


def wait_settled(expected: dict, expected_alerts: list[dict], timeout_s: float) -> tuple[dict, dict, float]:
    """Poll until results match the reference, or stop changing; return the final comparison."""
    t0, last, stable_since = time.monotonic(), None, time.monotonic()
    while True:
        got = read_results()
        cmp = compare(expected, expected_alerts, got)
        exact = cmp["windows_missing"] == cmp["windows_extra"] == cmp["windows_mismatched"] == 0 \
            and cmp["late_match"] and cmp["dlq_match"] and cmp["alerts_match"] and cmp["window_operator_late"] == 0
        fingerprint = (got["window_writes"], len(got["late"]), len(got["alerts"]), sum(got["dlq"].values()))
        if fingerprint != last:
            last, stable_since = fingerprint, time.monotonic()
        if exact or time.monotonic() - stable_since > 20 or time.monotonic() - t0 > timeout_s:
            return got, {**cmp, "exact": exact}, time.monotonic() - t0
        time.sleep(1)


# --- crash injection ---------------------------------------------------------------------------

def crash(kind: str, job_id: str, topic: str, group: str) -> tuple[str, dict]:
    """Inject a failure and bring processing back. Returns (job id now running, details)."""
    t0 = time.monotonic()
    if kind == "taskmanager":
        # Recovery = kill until the job has actually failed over (restart counter increased) and every
        # task runs again. Waiting for RUNNING alone returned before the JobManager even noticed the
        # loss (10 s heartbeat timeout), giving a spurious 3.1 s.
        before = num_restarts(job_id)
        stack.kill_taskmanager()
        time.sleep(3)
        stack.taskmanager_up()
        deadline = time.monotonic() + 120
        while num_restarts(job_id) <= before and time.monotonic() < deadline:
            time.sleep(0.5)
        detected_s = time.monotonic() - t0
        wait_running(job_id)
        return job_id, {"recovery_s": round(time.monotonic() - t0, 1), "failover_detected_s": round(detected_s, 1)}
    if kind == "job":
        checkpoint = wait_checkpoint(job_id)
        cancel(job_id)
        new_id = submit(topic, group, from_checkpoint=checkpoint)
        wait_running(new_id)
        return new_id, {"recovery_s": round(time.monotonic() - t0, 1), "restored_from": checkpoint}
    if kind == "postgres":
        stack.postgres_down()
        time.sleep(8)  # sink writes fail meanwhile; the job fails over and keeps restarting
        stack.postgres_up()
        wait_running(job_id, timeout_s=180)
        return job_id, {"recovery_s": round(time.monotonic() - t0, 1)}
    raise ValueError(kind)


# --- main --------------------------------------------------------------------------------------

def cut_index(records: list[dict], at: float, before_very_late: bool) -> int:
    cut = int(len(records) * at)
    if before_very_late:  # adversarial: the first record after recovery is a very-late event
        cut = next(i for i in range(cut, len(records)) if records[i]["kind"] == "very_late")
    return cut


def run_once(seed: int, crash_kind: str | None, at: float, rate: float, duration_s: int, label: str,
             before_very_late: bool = False) -> dict:
    run_id = f"{datetime.now(UTC):%Y%m%dT%H%M%SZ}-s{seed}-{crash_kind or 'clean'}-{uuid.uuid4().hex[:4]}"
    topic, group = f"events-{run_id}", f"pipeline-{run_id}"
    meta, records = generate(Config(seed=seed, duration_s=duration_s))
    expected = compute([r["value"] for r in records])
    expected_alerts = reference_alerts(expected["windows"])

    stack.create_topic(topic, partitions=meta["config"]["partitions"])
    stack.truncate_tables()
    clean_slate()
    job_id = submit(topic, group)
    try:
        wait_running(job_id)
        check_ordering_chain(job_id)
        cut = cut_index(records, at, before_very_late) if crash_kind else len(records)
        produced = [produce(records[:cut], topic, rate)]
        crash_info = None
        if crash_kind:
            time.sleep(8)  # input before the cut is consumed and covered by a completed checkpoint (3 s interval)
            job_id, crash_info = crash(crash_kind, job_id, topic, group)
            produced.append(produce(records[cut:], topic, rate))
        got, cmp, settle_s = wait_settled(expected, expected_alerts, timeout_s=300)
    finally:
        cancel(job_id)
    return {"run_id": run_id, "label": label, "seed": seed, "crash": crash_kind, "crash_at": at if crash_kind else None,
            "cut_before_very_late": before_very_late, "cut_index": cut,
            "records": len(records), "produce": produced, "crash_info": crash_info, "settle_s": round(settle_s, 1),
            "window_writes": got["window_writes"], **cmp}


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--seed", type=int, default=1)
    p.add_argument("--crash", choices=["taskmanager", "job", "postgres"])
    p.add_argument("--at", type=float, default=0.5, help="fraction of input produced before the crash")
    p.add_argument("--rate", type=float, default=4000, help="records/s produced")
    p.add_argument("--duration-s", type=int, default=600)
    p.add_argument("--label", default="")
    p.add_argument("--before-very-late", action="store_true",
                   help="move the crash point to just before a very-late event (adversarial restore test)")
    args = p.parse_args()
    stack.up()
    result = run_once(args.seed, args.crash, args.at, args.rate, args.duration_s, args.label, args.before_very_late)
    out = ROOT / "results/raw/pipeline_runs.jsonl"
    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, "a") as f:
        f.write(json.dumps(result) + "\n")
    print(json.dumps({k: v for k, v in result.items() if k not in ("produce", "mismatch_examples")}, indent=1))
    if result["mismatch_examples"]:
        print("mismatch examples:", json.dumps(result["mismatch_examples"]))


if __name__ == "__main__":
    main()
