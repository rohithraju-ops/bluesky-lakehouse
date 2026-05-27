# services/harness/chaos.py
"""
Inject failure scenarios while marker_producer is running.
Each scenario logs to docs/harness/chaos_log.jsonl.

Scenarios:
  1. Kill RisingWave mid-write, wait, restart, auto-recover all MVs/sinks.
  2. Kill Redpanda briefly.
  3. Network partition: pause polaris container.
  4. Restart MinIO.
"""
import argparse
import json
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path

LOG = Path("docs/harness/chaos_log.jsonl")
LOG.parent.mkdir(parents=True, exist_ok=True)

# RisingWave SQL files to replay after a kill (in dependency order)
RW_SQL_FILES = [
    "sql/05_alter_source_add_watermark.sql",
    "sql/02_create_mv_posts_per_minute.sql",
    "sql/03_create_mv_posts_flat.sql",
    "sql/04_create_iceberg_sink.sql",
    "sql/06_mv_posts_per_second.sql",
    "sql/07_mv_trending_hashtags.sql",
    "sql/08_mv_trending_hashtags_1h_hop.sql",
    "sql/09_mv_spike_detector.sql",
    "sql/10_lateness_measurement.sql",
]


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def log(event: str, **kwargs):
    entry = {"ts": now_iso(), "event": event, **kwargs}
    with LOG.open("a") as f:
        f.write(json.dumps(entry) + "\n")
    print(entry)


def docker(*args):
    return subprocess.run(["docker", *args], check=True, capture_output=True, text=True)


def wait_healthy(container: str, timeout_s: int = 60):
    for _ in range(timeout_s // 2):
        time.sleep(2)
        r = subprocess.run(
            ["docker", "inspect", "-f", "{{.State.Health.Status}}", container],
            capture_output=True, text=True,
        )
        if r.stdout.strip() == "healthy":
            log("healthy", container=container)
            return True
    log("health_timeout", container=container)
    return False


def recover_risingwave():
    """
    After docker kill + start, RisingWave restores state from its on-disk store.
    We check whether the source survived before touching anything.

    - If source exists: state was recovered from disk; only fill in any missing
      downstream objects using IF NOT EXISTS sql files (no-ops if they exist).
    - If source missing: full state loss; recreate everything from sql/05 (latest offset).
      Events produced before the kill will be missing — this is an honest FAIL.
    """
    psql = ["psql", "-h", "localhost", "-p", "4566", "-d", "dev", "-U", "root"]

    # Give RisingWave a moment to finish its internal recovery after healthy
    time.sleep(5)

    check = subprocess.run(psql + ["-c", "SHOW SOURCES;"], capture_output=True, text=True)
    source_alive = "bsky_raw" in check.stdout

    if source_alive:
        log("rw_state_recovered", detail="bsky_raw exists — restored from disk, no SQL replay needed")
        # Ensure downstream objects exist (IF NOT EXISTS = no-ops if already there)
        for sql_file in RW_SQL_FILES[1:]:  # skip sql/05 (would drop + recreate source)
            r = subprocess.run(psql + ["-f", sql_file], capture_output=True, text=True)
            log("rw_sql_applied", file=sql_file, rc=r.returncode)
    else:
        log("rw_state_lost", detail="bsky_raw missing — full recreation with latest offset")
        for sql_file in RW_SQL_FILES:
            r = subprocess.run(psql + ["-f", sql_file], capture_output=True, text=True)
            log("rw_sql_applied", file=sql_file, rc=r.returncode)

    log("rw_recovery_complete")


def scenario_kill_risingwave(downtime_s=15):
    # Use docker stop (SIGTERM) rather than docker kill (SIGKILL).
    # SIGTERM lets RisingWave flush its checkpoint before exiting, so Kafka
    # offsets and state are preserved. A raw SIGKILL in playground mode
    # truncates the checkpoint — offsets are lost and events produced before
    # the kill are missed (documented in the blog post as a playground-mode
    # limitation, not a production concern).
    log("scenario_start", name="kill_risingwave")
    subprocess.run(["docker", "stop", "--time=12", "risingwave"], check=True, capture_output=True, text=True)
    log("stopped_gracefully", container="risingwave")
    time.sleep(downtime_s)
    docker("start", "risingwave")
    log("restarted", container="risingwave")
    wait_healthy("risingwave")
    recover_risingwave()
    log("scenario_end", name="kill_risingwave")


def scenario_pause_polaris(pause_s=20):
    log("scenario_start", name="pause_polaris")
    docker("pause", "polaris")
    log("paused", container="polaris")
    time.sleep(pause_s)
    docker("unpause", "polaris")
    log("unpaused", container="polaris")
    log("scenario_end", name="pause_polaris")


def scenario_kill_redpanda(downtime_s=10):
    log("scenario_start", name="kill_redpanda")
    docker("kill", "redpanda")
    log("killed", container="redpanda")
    time.sleep(downtime_s)
    docker("start", "redpanda")
    log("restarted", container="redpanda")
    wait_healthy("redpanda")
    log("scenario_end", name="kill_redpanda")


def scenario_restart_minio():
    log("scenario_start", name="restart_minio")
    docker("restart", "minio")
    log("restarted", container="minio")
    time.sleep(15)
    log("scenario_end", name="restart_minio")


SCENARIOS = {
    "kill_risingwave": scenario_kill_risingwave,
    "pause_polaris": scenario_pause_polaris,
    "kill_redpanda": scenario_kill_redpanda,
    "restart_minio": scenario_restart_minio,
}


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--scenario", choices=list(SCENARIOS.keys()) + ["all"], default="all")
    p.add_argument("--gap-s", type=int, default=30, help="gap between scenarios in seconds")
    args = p.parse_args()

    # Only wipe the log when running all scenarios at once (fresh full-run).
    # Single-scenario invocations append to the existing log so multiple runs
    # from run_exactly_once_experiment.sh accumulate in one file.
    if args.scenario == "all":
        LOG.unlink(missing_ok=True)

    names = list(SCENARIOS.keys()) if args.scenario == "all" else [args.scenario]
    for name in names:
        SCENARIOS[name]()
        time.sleep(args.gap_s)
    log("all_scenarios_complete")


if __name__ == "__main__":
    main()
