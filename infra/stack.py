"""Local stack: PostgreSQL, Kafka (KRaft, single node) and a Flink standalone cluster
(1 JobManager + 1 TaskManager), all as user-space processes from .tools/ (no containers yet).

    python -m infra.stack up | down | status | kill-taskmanager | start-taskmanager

Every listener has an explicit port below the kernel's ephemeral range (32768-60999), avoiding the
bind collision project 02 hit. Processes are detached with pid files, so crash tests can SIGKILL
individual components and the stack outlives the command that started it.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import signal
import stat
import subprocess
import sys
import time
import uuid
from pathlib import Path

import httpx
import psycopg

ROOT = Path(__file__).resolve().parents[1]
TOOLS = ROOT / ".tools"
RUN = Path(os.environ.get("STACK_RUN_DIR", ROOT / ".run"))
LOGS, PIDS = RUN / "logs", RUN / "pids"
def _java_home() -> Path:
    """The JDK from scripts/setup_tools.sh (Linux) if present, else $JAVA_HOME (e.g. Homebrew openjdk@17 on macOS)."""
    local = TOOLS / "jdk"
    if (local / "bin/java").exists() or not os.environ.get("JAVA_HOME"):
        return local
    return Path(os.environ["JAVA_HOME"])


def _pg_bin() -> Path:
    """$PG_BIN, else the Linux user-space copy, else `pg_config` (Homebrew's PostgreSQL on macOS)."""
    if os.environ.get("PG_BIN"):
        return Path(os.environ["PG_BIN"])
    local = TOOLS / "pg/usr/lib/postgresql/16/bin"
    if (local / "postgres").exists():
        return local
    pg_config = shutil.which("pg_config")
    if pg_config:
        return Path(subprocess.run([pg_config, "--bindir"], capture_output=True, text=True).stdout.strip())
    return local


JAVA_HOME = _java_home()
PG_BIN, PG_LIB = _pg_bin(), TOOLS / "pg/usr/lib/aarch64-linux-gnu"   # PG_LIB: Linux copy only
PG_LOCALE = "C" if sys.platform == "darwin" else "C.UTF-8"           # macOS has no C.UTF-8 locale

BASE = int(os.environ.get("STACK_PORT_BASE", "19000"))
PG_PORT, KAFKA_PORT, KAFKA_CONTROLLER_PORT = BASE + 432, BASE + 92, BASE + 93
FLINK_REST_PORT, FLINK_RPC_PORT, FLINK_BLOB_PORT = BASE + 81, BASE + 123, BASE + 125
FLINK_TM_RPC_PORTS, FLINK_TM_DATA_PORT, FLINK_QUERY_PORTS = f"{BASE + 130}-{BASE + 139}", BASE + 140, f"{BASE + 150}-{BASE + 159}"

BOOTSTRAP = f"127.0.0.1:{KAFKA_PORT}"
FLINK_URL = f"http://127.0.0.1:{FLINK_REST_PORT}"
JDBC_URL = f"jdbc:postgresql://127.0.0.1:{PG_PORT}/stream?user=stream"
DSN = f"host=127.0.0.1 port={PG_PORT} dbname=stream user=stream connect_timeout=3"

SCHEMA = """
CREATE TABLE IF NOT EXISTS windows (
    service text NOT NULL, window_start_ms bigint NOT NULL,
    request_count int NOT NULL, error_count int NOT NULL,
    latency_p95_ms double precision NOT NULL, queue_depth_max int NOT NULL,
    first_written_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    last_written_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    writes int NOT NULL DEFAULT 1,
    PRIMARY KEY (service, window_start_ms));
CREATE TABLE IF NOT EXISTS late_events (
    event_id text PRIMARY KEY, service text NOT NULL, window_start_ms bigint NOT NULL);
CREATE TABLE IF NOT EXISTS window_operator_late (event_id text PRIMARY KEY);  -- tripwire: must stay empty
CREATE TABLE IF NOT EXISTS dlq (
    kafka_partition int NOT NULL, kafka_offset bigint NOT NULL, reason text NOT NULL, raw text NOT NULL,
    PRIMARY KEY (kafka_partition, kafka_offset));
CREATE TABLE IF NOT EXISTS alerts (
    alert_id text PRIMARY KEY, service text NOT NULL, episode_start_ms bigint NOT NULL,
    opened_window_ms bigint NOT NULL, closed_window_ms bigint, metrics text NOT NULL,
    z_at_open double precision NOT NULL,
    first_written_at timestamptz NOT NULL DEFAULT clock_timestamp());
"""


# --- processes ---------------------------------------------------------------------------------

def _proc_state(pid: int) -> str:
    """One-letter process state ('X' = gone). /proc on Linux, ps elsewhere (macOS has no /proc)."""
    if os.path.isdir("/proc"):
        try:
            with open(f"/proc/{pid}/stat") as f:
                return f.read().split(") ", 1)[1][0]
        except (FileNotFoundError, IndexError):
            return "X"
    out = subprocess.run(["ps", "-o", "stat=", "-p", str(pid)], capture_output=True, text=True).stdout.strip()
    return out[:1] if out else "X"


def _alive(pid: int) -> bool:
    try:
        os.waitpid(pid, os.WNOHANG)
    except ChildProcessError:
        pass
    return _proc_state(pid) not in ("Z", "X")


def _pid(name: str) -> int | None:
    try:
        return json.loads((PIDS / f"{name}.json").read_text())["pid"]
    except (FileNotFoundError, ValueError):
        return None


def running(name: str) -> bool:
    pid = _pid(name)
    return pid is not None and _alive(pid)


def spawn(name: str, cmd: list[str], env: dict | None = None) -> None:
    if running(name):
        return
    LOGS.mkdir(parents=True, exist_ok=True)
    PIDS.mkdir(parents=True, exist_ok=True)
    out = open(LOGS / f"{name}.out", "a")
    proc = subprocess.Popen(cmd, cwd=ROOT, env={**os.environ, "JAVA_HOME": str(JAVA_HOME), **(env or {})},
                            stdout=out, stderr=subprocess.STDOUT, start_new_session=True)
    (PIDS / f"{name}.json").write_text(json.dumps({"pid": proc.pid, "started_at": time.time()}))


def stop(name: str, sig: int = signal.SIGTERM, grace_s: float = 15.0) -> None:
    pid = _pid(name)
    if pid and _alive(pid):
        try:
            os.killpg(pid, sig)
        except ProcessLookupError:
            pass
        deadline = time.monotonic() + grace_s
        while _alive(pid) and time.monotonic() < deadline:
            time.sleep(0.1)
        if _alive(pid):
            os.killpg(pid, signal.SIGKILL)
            while _alive(pid):
                time.sleep(0.05)
    (PIDS / f"{name}.json").unlink(missing_ok=True)


def wait_for(check, what: str, timeout_s: float = 60) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        try:
            if check():
                return
        except Exception:  # noqa: BLE001 - any failure just means "not ready yet"
            pass
        time.sleep(0.5)
    raise RuntimeError(f"{what} not ready after {timeout_s}s; see {LOGS}")


# --- PostgreSQL (as `nobody` when root, like project 02) ---------------------------------------

def _pg_prefix() -> list[str]:
    if os.geteuid() != 0:
        return []
    pg_dir = RUN / "pg"
    groups = {p.stat().st_gid for p in [pg_dir, *pg_dir.parents] if p.exists() and not p.stat().st_mode & stat.S_IXOTH}
    return ["setpriv", "--reuid=65534", "--regid=65534", f"--groups={','.join(map(str, sorted(groups))) or '65534'}"]


def _pg(tool: str, *args: str, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run([*_pg_prefix(), str(PG_BIN / tool), *args], env={**os.environ, "LD_LIBRARY_PATH": str(PG_LIB)},
                          capture_output=True, text=True, check=check)


def postgres_up() -> None:
    pg_dir, data = RUN / "pg", RUN / "pg/data"
    if not data.exists():
        pg_dir.mkdir(parents=True, exist_ok=True)
        if os.geteuid() == 0:
            os.chown(pg_dir, 65534, 65534)
        _pg("initdb", "-D", str(data), "-U", "postgres", "--auth=trust", "-E", "UTF8", f"--locale={PG_LOCALE}")
    if _pg("pg_isready", "-h", "127.0.0.1", "-p", str(PG_PORT), check=False).returncode != 0:
        _pg("pg_ctl", "-D", str(data), "-l", str(pg_dir / "postgres.log"), "-w", "start",
            "-o", f"-p {PG_PORT} -k {pg_dir} -c listen_addresses=127.0.0.1")
    admin = f"host=127.0.0.1 port={PG_PORT} dbname=postgres user=postgres"
    with psycopg.connect(admin, autocommit=True) as conn:
        if not conn.execute("SELECT 1 FROM pg_roles WHERE rolname='stream'").fetchone():
            conn.execute("CREATE ROLE stream LOGIN")
        if not conn.execute("SELECT 1 FROM pg_database WHERE datname='stream'").fetchone():
            conn.execute("CREATE DATABASE stream OWNER stream")
    with psycopg.connect(DSN, autocommit=True) as conn:
        conn.execute(SCHEMA)


def postgres_down() -> None:
    if (RUN / "pg/data").exists():
        _pg("pg_ctl", "-D", str(RUN / "pg/data"), "-m", "fast", "-w", "stop", check=False)


def truncate_tables() -> None:
    with psycopg.connect(DSN, autocommit=True) as conn:
        conn.execute("TRUNCATE windows, late_events, window_operator_late, dlq, alerts")


# --- Kafka (KRaft) -----------------------------------------------------------------------------

def kafka_up() -> None:
    kdir = RUN / "kafka"
    props = kdir / "server.properties"
    if not props.exists():
        kdir.mkdir(parents=True, exist_ok=True)
        props.write_text("\n".join([
            "process.roles=broker,controller", "node.id=1",
            f"controller.quorum.voters=1@127.0.0.1:{KAFKA_CONTROLLER_PORT}",
            f"listeners=PLAINTEXT://127.0.0.1:{KAFKA_PORT},CONTROLLER://127.0.0.1:{KAFKA_CONTROLLER_PORT}",
            f"advertised.listeners=PLAINTEXT://127.0.0.1:{KAFKA_PORT}",
            "controller.listener.names=CONTROLLER", "inter.broker.listener.name=PLAINTEXT",
            "listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT",
            f"log.dirs={kdir / 'data'}", "num.partitions=3",
            "offsets.topic.replication.factor=1", "transaction.state.log.replication.factor=1",
            "transaction.state.log.min.isr=1", "group.initial.rebalance.delay.ms=0",
            "log.retention.hours=168", "auto.create.topics.enable=false", ""]))
        subprocess.run([str(TOOLS / "kafka/bin/kafka-storage.sh"), "format", "-t", uuid.uuid4().hex[:22],
                        "-c", str(props)], env={**os.environ, "JAVA_HOME": str(JAVA_HOME)},
                       check=True, capture_output=True, text=True)
    spawn("kafka", [str(TOOLS / "kafka/bin/kafka-server-start.sh"), str(props)],
          {"LOG_DIR": str(LOGS / "kafka"), "KAFKA_HEAP_OPTS": "-Xms1g -Xmx1g"})

    def ready() -> bool:
        from confluent_kafka.admin import AdminClient
        return bool(AdminClient({"bootstrap.servers": BOOTSTRAP}).list_topics(timeout=2).brokers)
    wait_for(ready, "kafka")


def create_topic(name: str, partitions: int = 3) -> None:
    from confluent_kafka.admin import AdminClient, NewTopic
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    if name in admin.list_topics(timeout=5).topics:
        return
    for future in admin.create_topics([NewTopic(name, num_partitions=partitions, replication_factor=1)]).values():
        future.result(timeout=30)


# --- Flink -------------------------------------------------------------------------------------

def _shipped_java_opts() -> str:
    match = re.search(r"^\s+all: (--add-.*)$", (TOOLS / "flink/conf/config.yaml").read_text(), re.MULTILINE)
    if not match:
        raise RuntimeError("env.java.opts.all not found in the Flink distribution config")
    return match.group(1).strip()


def flink_conf_dir() -> Path:
    conf = RUN / "flink-conf"
    conf.mkdir(parents=True, exist_ok=True)
    for f in (TOOLS / "flink/conf").iterdir():
        if f.name.startswith("log4j") and not (conf / f.name).exists():
            (conf / f.name).write_text(f.read_text())
    settings = {
        "jobmanager.rpc.address": "127.0.0.1", "jobmanager.rpc.port": FLINK_RPC_PORT,
        "jobmanager.bind-host": "127.0.0.1", "jobmanager.memory.process.size": "1600m",
        "taskmanager.bind-host": "127.0.0.1", "taskmanager.host": "127.0.0.1",
        "taskmanager.rpc.port": FLINK_TM_RPC_PORTS, "taskmanager.data.port": FLINK_TM_DATA_PORT,
        "taskmanager.numberOfTaskSlots": 8, "taskmanager.memory.process.size": "3072m",
        "rest.port": FLINK_REST_PORT, "rest.address": "127.0.0.1", "rest.bind-address": "127.0.0.1",
        "blob.server.port": str(FLINK_BLOB_PORT), "metrics.internal.query-service.port": FLINK_QUERY_PORTS,
        "parallelism.default": 2,
        "state.backend.type": "hashmap",
        "execution.checkpointing.dir": f"file://{RUN / 'checkpoints'}",
        "execution.checkpointing.savepoint-dir": f"file://{RUN / 'savepoints'}",
        "execution.checkpointing.externalized-checkpoint-retention": "RETAIN_ON_CANCELLATION",
        "execution.checkpointing.num-retained": 3,
        "restart-strategy.type": "fixed-delay",
        "restart-strategy.fixed-delay.attempts": 1000, "restart-strategy.fixed-delay.delay": "2 s",
        "heartbeat.timeout": 10000, "heartbeat.interval": 2000,  # notice a killed TaskManager in ~10 s
        # Java 17 module access flags Flink needs, carried over from the distribution's own config
        # (dropping them breaks job submission with InaccessibleObjectException).
        "env.java.opts.all": _shipped_java_opts(),
    }
    (conf / "config.yaml").write_text("".join(f"{k}: {json.dumps(v) if isinstance(v, str) else v}\n" for k, v in settings.items()))
    return conf


def _flink_env() -> dict:
    return {"FLINK_CONF_DIR": str(flink_conf_dir()), "FLINK_LOG_DIR": str(LOGS / "flink")}


def jobmanager_up() -> None:
    spawn("flink-jobmanager", [str(TOOLS / "flink/bin/jobmanager.sh"), "start-foreground"], _flink_env())
    wait_for(lambda: httpx.get(f"{FLINK_URL}/overview", timeout=2).status_code == 200, "flink jobmanager")


def taskmanager_up(wait: bool = True) -> None:
    spawn("flink-taskmanager", [str(TOOLS / "flink/bin/taskmanager.sh"), "start-foreground"], _flink_env())
    if wait:
        wait_for(lambda: httpx.get(f"{FLINK_URL}/overview", timeout=2).json()["taskmanagers"] >= 1,
                 "flink taskmanager")


def kill_taskmanager() -> None:
    """Crash test: SIGKILL, no graceful shutdown."""
    stop("flink-taskmanager", sig=signal.SIGKILL, grace_s=0)


def up() -> None:
    for d in (LOGS, PIDS, RUN / "checkpoints", RUN / "savepoints"):
        d.mkdir(parents=True, exist_ok=True)
    postgres_up()
    kafka_up()
    jobmanager_up()
    taskmanager_up()


def down() -> None:
    for name in ("flink-taskmanager", "flink-jobmanager", "kafka"):
        stop(name)
    postgres_down()


def status() -> dict:
    return {name: running(name) for name in ("kafka", "flink-jobmanager", "flink-taskmanager")} | {
        "postgres": _pg("pg_isready", "-h", "127.0.0.1", "-p", str(PG_PORT), check=False).returncode == 0}


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("command", choices=["up", "down", "status", "kill-taskmanager", "start-taskmanager"])
    args = p.parse_args()
    {"up": up, "down": down, "status": lambda: None, "kill-taskmanager": kill_taskmanager,
     "start-taskmanager": taskmanager_up}[args.command]()
    print(json.dumps(status()))


if __name__ == "__main__":
    sys.exit(main())
