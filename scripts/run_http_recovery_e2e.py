#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import signal
import socket
import subprocess
import sys
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import pymysql
import requests


DEFAULT_ENV_FILE = "/Users/chenmingdong01/Documents/密钥/test.env"
GROUP_CODE = "codex_http_recovery_test"
ROUTE = "codex-http-route"
BASE_BIZ_PREFIX = "codex-http-recovery"


@dataclass
class SubmittedTask:
    task_id: int
    biz_key: str
    user_id: str
    priority: int
    scheduled_future: bool


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Start real demo-consumer service(s), submit tasks over HTTP, flush Redis, and verify recovery via MySQL.")
    parser.add_argument("--env-file", default=DEFAULT_ENV_FILE)
    parser.add_argument("--ports", default="18088,18089,18090")
    parser.add_argument("--build", action="store_true", help="Run mvn package before starting services")
    parser.add_argument("--skip-build", action="store_true")
    parser.add_argument("--group-concurrency", type=int, default=6)
    parser.add_argument("--user-concurrency", type=int, default=2)
    parser.add_argument("--dispatch-batch-size", type=int, default=100)
    parser.add_argument("--worker-threads", type=int, default=8)
    parser.add_argument("--task-sleep-ms", type=int, default=5000)
    parser.add_argument("--future-delay-sec", type=int, default=12)
    parser.add_argument("--submit-timeout-sec", type=int, default=30)
    parser.add_argument("--run-timeout-sec", type=int, default=120)
    parser.add_argument("--poll-interval-ms", type=int, default=200)
    return parser.parse_args()


def load_env_file(path: str) -> dict[str, str]:
    env = {}
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            text = line.strip()
            if not text or text.startswith("#") or "=" not in text:
                continue
            key, value = text.split("=", 1)
            env[key.strip()] = value.strip()
    return env


def parse_ports(text: str) -> list[int]:
    return [int(item.strip()) for item in text.split(",") if item.strip()]


def wait_for_port(port: int, timeout_sec: int) -> None:
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(0.5)
            if sock.connect_ex(("127.0.0.1", port)) == 0:
                return
        time.sleep(0.2)
    raise TimeoutError(f"port {port} was not ready within {timeout_sec}s")


def resolve_jar(repo_root: Path) -> Path:
    jars = sorted(
        [
            p for p in (repo_root / "demo-consumer" / "target").glob("demo-consumer-*.jar")
            if not p.name.endswith(".original")
        ],
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if not jars:
        raise SystemExit("No runnable jar found under demo-consumer/target")
    return jars[0]


def build_if_needed(repo_root: Path, env: dict[str, str], args: argparse.Namespace) -> None:
    if args.build and args.skip_build:
        raise SystemExit("Use either --build or --skip-build, not both")
    if args.skip_build:
        return
    cmd = ["mvn", "-DskipTests", "-pl", "demo-consumer", "-am", "package"]
    run(cmd, cwd=repo_root, env=env)


def run(cmd: list[str], cwd: Path, env: dict[str, str] | None = None) -> None:
    proc = subprocess.run(cmd, cwd=str(cwd), env=env, check=False)
    if proc.returncode != 0:
        raise SystemExit(f"command failed({proc.returncode}): {' '.join(cmd)}")


def parse_jdbc_url(jdbc_url: str) -> tuple[str, int, str]:
    if not jdbc_url.startswith("jdbc:mysql://"):
        raise SystemExit(f"Unsupported MYSQL_URL: {jdbc_url}")
    parsed = urlparse(jdbc_url[5:])
    db_name = parsed.path.lstrip("/")
    if not db_name:
        raise SystemExit(f"Database name missing in MYSQL_URL: {jdbc_url}")
    return parsed.hostname or "127.0.0.1", parsed.port or 3306, db_name


def mysql_conn(env_vars: dict[str, str]):
    host, port, database = parse_jdbc_url(env_vars["MYSQL_URL"])
    return pymysql.connect(
        host=host,
        port=port,
        user=env_vars["MYSQL_USERNAME"],
        password=env_vars["MYSQL_PASSWORD"],
        database=database,
        charset="utf8mb4",
        autocommit=True,
        cursorclass=pymysql.cursors.DictCursor,
    )


def ensure_group_config(conn, args: argparse.Namespace) -> None:
    sql = """
    INSERT INTO scheduler_group_config(
        group_code, enabled, max_concurrency, user_base_concurrency,
        dynamic_user_limit_enabled, load_strategy_json,
        dispatch_batch_size, heartbeat_timeout_sec, lock_expire_sec, description
    ) VALUES (
        %s, 1, %s, %s, 0, NULL, %s, 30, 60, %s
    )
    ON DUPLICATE KEY UPDATE
        enabled = VALUES(enabled),
        max_concurrency = VALUES(max_concurrency),
        user_base_concurrency = VALUES(user_base_concurrency),
        dynamic_user_limit_enabled = VALUES(dynamic_user_limit_enabled),
        load_strategy_json = VALUES(load_strategy_json),
        dispatch_batch_size = VALUES(dispatch_batch_size),
        heartbeat_timeout_sec = VALUES(heartbeat_timeout_sec),
        lock_expire_sec = VALUES(lock_expire_sec),
        description = VALUES(description)
    """
    with conn.cursor() as cursor:
        cursor.execute(
            sql,
            (
                GROUP_CODE,
                args.group_concurrency,
                args.user_concurrency,
                args.dispatch_batch_size,
                "codex http recovery e2e",
            ),
        )


def cleanup_test_data(conn) -> None:
    like_prefix = f"{BASE_BIZ_PREFIX}%"
    with conn.cursor() as cursor:
        cursor.execute(
            "DELETE e FROM scheduler_task_execution e JOIN scheduler_task t ON e.task_id = t.id WHERE t.group_code = %s OR t.biz_key LIKE %s",
            (GROUP_CODE, like_prefix),
        )
        cursor.execute("DELETE FROM scheduler_task WHERE group_code = %s OR biz_key LIKE %s", (GROUP_CODE, like_prefix))
        cursor.execute("DELETE FROM demo_biz_task WHERE biz_key LIKE %s", (like_prefix,))


def start_services(repo_root: Path, env_vars: dict[str, str], args: argparse.Namespace) -> list[subprocess.Popen]:
    ports = parse_ports(args.ports)
    jar = resolve_jar(repo_root)
    logs_dir = repo_root / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    procs: list[subprocess.Popen] = []
    child_env = os.environ.copy()
    child_env["DEMO_DB_URL"] = env_vars["MYSQL_URL"]
    child_env["DEMO_DB_USERNAME"] = env_vars["MYSQL_USERNAME"]
    child_env["DEMO_DB_PASSWORD"] = env_vars["MYSQL_PASSWORD"]
    child_env["SPRING_DATA_REDIS_HOST"] = env_vars["REDIS_HOST"]
    child_env["SPRING_DATA_REDIS_PORT"] = env_vars["REDIS_PORT"]
    if "JAVA_HOME" in os.environ:
        child_env["JAVA_HOME"] = os.environ["JAVA_HOME"]

    for port in ports:
        log_file = logs_dir / f"http-recovery-{port}.log"
        cmd = [
            "java",
            "-jar",
            str(jar),
            f"--server.port={port}",
            f"--utask.scheduler.instance-id=http-e2e-{port}",
            "--utask.scheduler.dispatch-enabled=true",
            f"--utask.scheduler.dispatch-route={ROUTE}",
            "--utask.scheduler.dispatch-interval-ms=200",
            "--utask.scheduler.recovery-interval-ms=2000",
            "--utask.scheduler.queue-refill-interval-ms=2000",
            "--utask.scheduler.heartbeat-interval-sec=2",
            "--utask.scheduler.default-retry-delay-sec=2",
            f"--utask.scheduler.worker-threads={args.worker_threads}",
        ]
        log_fp = open(log_file, "ab")
        proc = subprocess.Popen(
            cmd,
            cwd=str(repo_root),
            env=child_env,
            stdout=log_fp,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        proc._codex_log_fp = log_fp  # type: ignore[attr-defined]
        procs.append(proc)

    for port in ports:
        wait_for_port(port, 45)
    return procs


def stop_services(procs: list[subprocess.Popen]) -> None:
    for proc in procs:
        try:
            os.killpg(proc.pid, signal.SIGTERM)
        except Exception:
            try:
                proc.terminate()
            except Exception:
                pass
    deadline = time.time() + 10
    for proc in procs:
        while proc.poll() is None and time.time() < deadline:
            time.sleep(0.2)
        if proc.poll() is None:
            try:
                os.killpg(proc.pid, signal.SIGKILL)
            except Exception:
                proc.kill()
        log_fp = getattr(proc, "_codex_log_fp", None)
        if log_fp is not None:
            log_fp.close()


def submit_task(base_url: str, payload: dict[str, Any]) -> SubmittedTask:
    response = requests.post(f"{base_url}/demo/submit", json=payload, timeout=10)
    response.raise_for_status()
    data = response.json()
    return SubmittedTask(
        task_id=int(data["taskId"]),
        biz_key=str(data["bizKey"]),
        user_id=str(payload["userId"]),
        priority=int(payload["priority"]),
        scheduled_future=payload["executeAt"] is not None,
    )


def now_iso(offset_sec: int) -> str:
    return (datetime.now() + timedelta(seconds=offset_sec)).replace(microsecond=0).isoformat()


def build_submit_plan(args: argparse.Namespace) -> list[dict[str, Any]]:
    ext = json.dumps({"sleepMs": args.task_sleep_ms, "failBeforeSuccess": 0}, separators=(",", ":"))
    plan: list[dict[str, Any]] = []
    user_specs = {
        "heavy-user": {0: 8, 10: 8, 20: 8},
        "user-b": {0: 4, 10: 4, 20: 4},
        "user-c": {0: 4, 10: 4, 20: 4},
    }
    future_specs = {
        "heavy-user": {0: 2},
        "user-b": {10: 1},
        "user-c": {20: 1},
    }
    seq = 0
    for user_id, priorities in user_specs.items():
        for priority, count in priorities.items():
            for _ in range(count):
                plan.append(
                    {
                        "groupCode": GROUP_CODE,
                        "userId": user_id,
                        "bizKey": f"{BASE_BIZ_PREFIX}-{seq:04d}",
                        "priority": priority,
                        "executeTimeoutSec": max(30, args.task_sleep_ms // 1000 + 10),
                        "retryDelaySec": 2,
                        "executeAt": None,
                        "extInfo": ext,
                        "payload": json.dumps({"seq": seq, "userId": user_id, "priority": priority}),
                    }
                )
                seq += 1
    for user_id, priorities in future_specs.items():
        for priority, count in priorities.items():
            for _ in range(count):
                plan.append(
                    {
                        "groupCode": GROUP_CODE,
                        "userId": user_id,
                        "bizKey": f"{BASE_BIZ_PREFIX}-future-{seq:04d}",
                        "priority": priority,
                        "executeTimeoutSec": max(30, args.task_sleep_ms // 1000 + 10),
                        "retryDelaySec": 2,
                        "executeAt": now_iso(args.future_delay_sec),
                        "extInfo": ext,
                        "payload": json.dumps({"seq": seq, "userId": user_id, "priority": priority, "future": True}),
                    }
                )
                seq += 1
    return plan


def fetch_task_rows(conn) -> list[dict[str, Any]]:
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT id, biz_key, user_id, priority, status, execute_at, start_time, finish_time, create_time
              FROM scheduler_task
             WHERE group_code = %s
             ORDER BY id ASC
            """,
            (GROUP_CODE,),
        )
        return list(cursor.fetchall())


def fetch_execution_rows(conn) -> list[dict[str, Any]]:
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT t.id AS task_id, t.user_id, t.priority, t.biz_key,
                   e.start_time, e.finish_time, e.status
              FROM scheduler_task_execution e
              JOIN scheduler_task t ON t.id = e.task_id
             WHERE t.group_code = %s
             ORDER BY e.start_time ASC, e.id ASC
            """,
            (GROUP_CODE,),
        )
        return list(cursor.fetchall())


def flush_redis_db(host: str, port: int) -> None:
    def encode(*parts: str) -> bytes:
        payload = [f"*{len(parts)}\r\n".encode("utf-8")]
        for part in parts:
            raw = part.encode("utf-8")
            payload.append(f"${len(raw)}\r\n".encode("utf-8"))
            payload.append(raw + b"\r\n")
        return b"".join(payload)

    with socket.create_connection((host, port), timeout=5) as sock:
        sock.sendall(encode("FLUSHDB"))
        response = sock.recv(1024)
        if not response.startswith(b"+OK"):
            raise RuntimeError(f"unexpected redis response: {response!r}")


def observe_and_wait(conn, args: argparse.Namespace, expected_total: int) -> dict[str, Any]:
    deadline = time.time() + args.run_timeout_sec
    poll_sec = max(0.1, args.poll_interval_ms / 1000.0)
    other_users_started = set()
    redis_flushed = False
    future_task_started = False

    while time.time() < deadline:
        rows = fetch_task_rows(conn)
        running_by_user: dict[str, int] = defaultdict(int)
        status_count: dict[str, int] = defaultdict(int)
        for row in rows:
            status = row["status"]
            status_count[status] += 1
            if status == "RUNNING":
                running_by_user[str(row["user_id"])] += 1
                if str(row["user_id"]) != "heavy-user":
                    other_users_started.add(str(row["user_id"]))
            if row["biz_key"].startswith(f"{BASE_BIZ_PREFIX}-future-") and row["start_time"] is not None:
                future_task_started = True

        current_group_running = sum(running_by_user.values())

        pending_backlog = status_count.get("RUNNABLE", 0) + status_count.get("PENDING", 0) + status_count.get("WAIT_RETRY", 0)
        if not redis_flushed and current_group_running > 0 and pending_backlog > 0:
            flush_redis_db(os.environ.get("REDIS_HOST_OVERRIDE", env_vars_global["REDIS_HOST"]), int(os.environ.get("REDIS_PORT_OVERRIDE", env_vars_global["REDIS_PORT"])))
            redis_flushed = True
            print(f"[info] redis flushed at {datetime.now().isoformat(timespec='seconds')}, group_running={current_group_running}")

        completed = status_count.get("SUCCESS", 0) + status_count.get("FAILED", 0) + status_count.get("CANCELLED", 0)
        if completed >= expected_total and status_count.get("RUNNING", 0) == 0 and status_count.get("RUNNABLE", 0) == 0 and status_count.get("WAIT_RETRY", 0) == 0 and status_count.get("PENDING", 0) == 0:
            return {
                "other_users_started": other_users_started,
                "redis_flushed": redis_flushed,
                "future_task_started": future_task_started,
                "final_rows": rows,
            }
        time.sleep(poll_sec)
    raise TimeoutError("tasks did not finish within timeout")


def verify_results(conn, args: argparse.Namespace, submitted: list[SubmittedTask], observe_result: dict[str, Any]) -> None:
    final_rows = observe_result["final_rows"]
    status_map = {int(row["id"]): str(row["status"]) for row in final_rows}
    future_task_ids = {task.task_id for task in submitted if task.scheduled_future}
    incomplete = [task.task_id for task in submitted if status_map.get(task.task_id) != "SUCCESS"]
    if incomplete:
        raise AssertionError(f"not all tasks succeeded, incomplete={incomplete[:10]}")

    if not observe_result["redis_flushed"]:
        raise AssertionError("redis was never flushed during the run")
    if not observe_result["future_task_started"]:
        raise AssertionError("future tasks never started after redis flush")

    execution_rows = fetch_execution_rows(conn)
    max_group_running = calculate_peak_concurrency(execution_rows)
    if max_group_running < args.group_concurrency:
        raise AssertionError(
            f"group concurrency not fully occupied, expected>={args.group_concurrency}, actual={max_group_running}"
        )

    execution_by_user: dict[str, list[dict[str, Any]]] = defaultdict(list)
    first_start_by_user_priority: dict[str, dict[int, datetime]] = defaultdict(dict)
    for row in execution_rows:
        execution_by_user[str(row["user_id"])].append(row)
        if int(row["task_id"]) in future_task_ids:
            continue
        start_time = row["start_time"]
        if start_time is None:
            continue
        first_start_by_user_priority[str(row["user_id"])].setdefault(int(row["priority"]), start_time)

    for user_id in ("heavy-user", "user-b", "user-c"):
        actual = calculate_peak_concurrency(execution_by_user.get(user_id, []))
        if actual < args.user_concurrency:
            raise AssertionError(f"user concurrency not full for {user_id}, expected>={args.user_concurrency}, actual={actual}")

    if not {"user-b", "user-c"}.issubset(set(execution_by_user)):
        raise AssertionError(f"other users were not all scheduled, actual={sorted(execution_by_user)}")

    for user_id in ("heavy-user", "user-b", "user-c"):
        user_map = first_start_by_user_priority.get(user_id, {})
        for priority in (0, 10, 20):
            if priority not in user_map:
                raise AssertionError(f"missing execution for user={user_id}, priority={priority}")
        if not (user_map[0] <= user_map[10] <= user_map[20]):
            raise AssertionError(
                f"priority order broken for user={user_id}, starts={{0:{user_map[0]} 10:{user_map[10]} 20:{user_map[20]}}}"
            )


def calculate_peak_concurrency(rows: list[dict[str, Any]]) -> int:
    events: list[tuple[datetime, int]] = []
    for row in rows:
        start_time = row.get("start_time")
        finish_time = row.get("finish_time")
        if start_time is None or finish_time is None:
            continue
        events.append((start_time, 1))
        events.append((finish_time, -1))
    events.sort(key=lambda item: (item[0], item[1]))
    current = 0
    peak = 0
    for _, delta in events:
        current += delta
        peak = max(peak, current)
    return peak


def print_summary(submitted: list[SubmittedTask], observe_result: dict[str, Any], conn) -> None:
    execution_rows = fetch_execution_rows(conn)
    execution_by_user: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in execution_rows:
        execution_by_user[str(row["user_id"])].append(row)
    print("=" * 72)
    print(f"submitted tasks      : {len(submitted)}")
    print(f"redis flushed        : {observe_result['redis_flushed']}")
    print(f"future task started  : {observe_result['future_task_started']}")
    print(f"max group running    : {calculate_peak_concurrency(execution_rows)}")
    print("max user running     :")
    for user_id in sorted(execution_by_user):
        print(f"  {user_id:<12} {calculate_peak_concurrency(execution_by_user[user_id])}")


def ensure_java21() -> None:
    if os.environ.get("JAVA_HOME"):
        return
    java_home_cmd = ["/usr/libexec/java_home", "-v", "21"]
    proc = subprocess.run(java_home_cmd, capture_output=True, text=True, check=False)
    if proc.returncode == 0:
        java_home = proc.stdout.strip()
        if java_home:
            os.environ["JAVA_HOME"] = java_home
            os.environ["PATH"] = f"{java_home}/bin:{os.environ.get('PATH', '')}"


env_vars_global: dict[str, str] = {}


def main() -> None:
    global env_vars_global
    args = parse_args()
    if not Path(args.env_file).exists():
        raise SystemExit(f"env file not found: {args.env_file}")
    repo_root = Path(__file__).resolve().parents[1]
    env_vars_global = load_env_file(args.env_file)
    for key in ("MYSQL_URL", "MYSQL_USERNAME", "MYSQL_PASSWORD", "REDIS_HOST", "REDIS_PORT"):
        if not env_vars_global.get(key):
            raise SystemExit(f"missing required env in {args.env_file}: {key}")

    ensure_java21()
    build_if_needed(repo_root, os.environ.copy(), args)

    conn = mysql_conn(env_vars_global)
    ensure_group_config(conn, args)
    cleanup_test_data(conn)

    services: list[subprocess.Popen] = []
    try:
        services = start_services(repo_root, env_vars_global, args)
        base_urls = [f"http://127.0.0.1:{port}" for port in parse_ports(args.ports)]
        plan = build_submit_plan(args)
        submitted: list[SubmittedTask] = []
        for index, payload in enumerate(plan):
            base_url = base_urls[index % len(base_urls)]
            submitted.append(submit_task(base_url, payload))

        observe_result = observe_and_wait(conn, args, len(submitted))
        verify_results(conn, args, submitted, observe_result)
        print_summary(submitted, observe_result, conn)
    finally:
        stop_services(services)
        conn.close()


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"[fatal] {exc}", file=sys.stderr)
        raise
