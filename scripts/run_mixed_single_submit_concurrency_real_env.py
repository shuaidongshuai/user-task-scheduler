#!/usr/bin/env python3
import glob
import json
import os
import signal
import socket
import subprocess
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import pymysql
import requests


DEFAULT_ENV_FILE = "/Users/chenmingdong01/Documents/密钥/test.env"
REPO_ROOT = Path(__file__).resolve().parents[1]
DEMO_PORT = int(os.environ.get("MIXED_CONCURRENCY_DEMO_PORT", "18109"))
GROUP_CODE = os.environ.get("MIXED_CONCURRENCY_GROUP", "codex_mixed_single_submit_real")
USER_ID = os.environ.get("MIXED_CONCURRENCY_USER", "codex-mixed-user")
RUN_ID = os.environ.get("MIXED_CONCURRENCY_RUN_ID", "codex-mixed-submit-" + uuid.uuid4().hex[:10])
INSTANCE_ID = os.environ.get("MIXED_CONCURRENCY_INSTANCE", "codex-mixed-submit-a")
APP_LOG = REPO_ROOT / "tmp" / f"{RUN_ID}-app.log"
APP_LOG.parent.mkdir(parents=True, exist_ok=True)

HOLD_TASK_COUNT = int(os.environ.get("MIXED_HOLD_TASK_COUNT", "1"))
NORMAL_TASK_COUNT = int(os.environ.get("MIXED_NORMAL_TASK_COUNT", "3"))
HOLD_PRIORITY = int(os.environ.get("MIXED_HOLD_PRIORITY", "0"))
NORMAL_PRIORITY = int(os.environ.get("MIXED_NORMAL_PRIORITY", "10"))
GROUP_CONCURRENCY = int(os.environ.get("MIXED_GROUP_CONCURRENCY", "1"))
USER_CONCURRENCY = int(os.environ.get("MIXED_USER_CONCURRENCY", "1"))
HOLD_RETRY_DELAY_SEC = int(os.environ.get("MIXED_HOLD_RETRY_DELAY_SEC", "5"))
NORMAL_SLEEP_MS = int(os.environ.get("MIXED_NORMAL_SLEEP_MS", "1200"))
HOLD_SUCCESS_SLEEP_MS = int(os.environ.get("MIXED_HOLD_SUCCESS_SLEEP_MS", "1200"))
DISPATCH_BATCH_SIZE = int(os.environ.get("MIXED_DISPATCH_BATCH_SIZE", "20"))


@dataclass
class DbConfig:
    host: str
    port: int
    database: str
    username: str
    password: str


class MiniRedis:
    def __init__(self, host: str, port: int) -> None:
        self.host = host
        self.port = port

    def command(self, *parts: Any):
        payload = self._encode(parts)
        with socket.create_connection((self.host, self.port), timeout=5) as sock:
            sock.sendall(payload)
            return self._read(sock)

    def get(self, key: str):
        return self.command("GET", key)

    def zscore(self, key: str, member: str):
        value = self.command("ZSCORE", key, member)
        return None if value is None else float(value)

    def delete(self, *keys: str):
        if keys:
            self.command("DEL", *keys)

    def keys(self, pattern: str):
        result = self.command("KEYS", pattern)
        return result or []

    def _encode(self, parts):
        out = [f"*{len(parts)}\r\n".encode()]
        for part in parts:
            data = str(part).encode()
            out.append(f"${len(data)}\r\n".encode())
            out.append(data + b"\r\n")
        return b"".join(out)

    def _read(self, sock):
        prefix = sock.recv(1)
        if prefix == b"+":
            return self._readline(sock)
        if prefix == b"-":
            raise RuntimeError(self._readline(sock))
        if prefix == b":":
            return int(self._readline(sock))
        if prefix == b"$":
            size = int(self._readline(sock))
            if size == -1:
                return None
            data = b""
            while len(data) < size + 2:
                data += sock.recv(size + 2 - len(data))
            return data[:-2].decode()
        if prefix == b"*":
            size = int(self._readline(sock))
            if size == -1:
                return None
            return [self._read(sock) for _ in range(size)]
        raise RuntimeError(f"unexpected redis prefix: {prefix!r}")

    def _readline(self, sock):
        data = b""
        while not data.endswith(b"\r\n"):
            data += sock.recv(1)
        return data[:-2].decode()


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


def ensure_runtime_env():
    env_file = os.environ.get("ENV_FILE", DEFAULT_ENV_FILE)
    file_env = load_env_file(env_file) if env_file and os.path.exists(env_file) else {}
    for key in ("MYSQL_URL", "MYSQL_USERNAME", "MYSQL_PASSWORD", "REDIS_HOST", "REDIS_PORT"):
        if not os.environ.get(key) and file_env.get(key):
            os.environ[key] = file_env[key]
    for key in ("MYSQL_URL", "MYSQL_USERNAME", "MYSQL_PASSWORD", "REDIS_HOST", "REDIS_PORT"):
        if not os.environ.get(key):
            raise RuntimeError(f"missing env: {key}")


def parse_mysql_jdbc(url: str) -> DbConfig:
    if not url.startswith("jdbc:mysql://"):
        raise ValueError(f"unsupported MYSQL_URL: {url}")
    parsed = urlparse(url[len("jdbc:"):])
    return DbConfig(
        host=parsed.hostname or "127.0.0.1",
        port=parsed.port or 3306,
        database=parsed.path.lstrip("/"),
        username=os.environ["MYSQL_USERNAME"],
        password=os.environ["MYSQL_PASSWORD"],
    )


def db_conn(cfg: DbConfig):
    return pymysql.connect(
        host=cfg.host,
        port=cfg.port,
        user=cfg.username,
        password=cfg.password,
        database=cfg.database,
        charset="utf8mb4",
        autocommit=True,
        cursorclass=pymysql.cursors.DictCursor,
    )


def ensure_schema(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            select column_name as column_name
              from information_schema.columns
             where table_schema = database()
               and table_name = 'scheduler_task'
               and column_name in ('hold_round_count', 'hold_max_rounds', 'hold_retry_delay_sec')
            """
        )
        cols = {row["column_name"] for row in cur.fetchall()}
        if cols != {"hold_round_count", "hold_max_rounds", "hold_retry_delay_sec"}:
            cur.execute(
                """
                ALTER TABLE scheduler_task
                    ADD COLUMN hold_round_count INT NOT NULL DEFAULT 0 COMMENT 'WAIT_HOLD 已经进入的轮次数' AFTER max_retry_count,
                    ADD COLUMN hold_max_rounds INT NOT NULL DEFAULT 1000 COMMENT 'WAIT_HOLD 最多允许轮次数' AFTER hold_round_count,
                    ADD COLUMN hold_retry_delay_sec INT NOT NULL DEFAULT 3 COMMENT 'WAIT_HOLD 每轮等待时间（秒）' AFTER hold_max_rounds
                """
            )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS demo_biz_task (
                id BIGINT PRIMARY KEY AUTO_INCREMENT,
                biz_key VARCHAR(128) NOT NULL UNIQUE,
                status VARCHAR(32) NOT NULL,
                payload TEXT DEFAULT NULL,
                create_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                update_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """
        )
        cur.execute(
            """
            INSERT INTO scheduler_group_config(
                group_code, enabled, max_concurrency, user_base_concurrency,
                dynamic_user_limit_enabled, load_strategy_json,
                dispatch_batch_size, heartbeat_timeout_sec, lock_expire_sec, description
            ) VALUES (%s,1,%s,%s,0,NULL,%s,30,60,'mixed single submit real env test')
            ON DUPLICATE KEY UPDATE
                enabled=VALUES(enabled),
                max_concurrency=VALUES(max_concurrency),
                user_base_concurrency=VALUES(user_base_concurrency),
                dynamic_user_limit_enabled=VALUES(dynamic_user_limit_enabled),
                load_strategy_json=VALUES(load_strategy_json),
                dispatch_batch_size=VALUES(dispatch_batch_size),
                heartbeat_timeout_sec=VALUES(heartbeat_timeout_sec),
                lock_expire_sec=VALUES(lock_expire_sec),
                description=VALUES(description)
            """,
            (GROUP_CODE, GROUP_CONCURRENCY, USER_CONCURRENCY, DISPATCH_BATCH_SIZE),
        )


def cleanup(conn):
    with conn.cursor() as cur:
        cur.execute("select id from scheduler_task where biz_key like %s", (RUN_ID + "%",))
        ids = [row["id"] for row in cur.fetchall()]
        if ids:
            placeholders = ",".join(["%s"] * len(ids))
            cur.execute(f"delete from scheduler_task_execution where task_id in ({placeholders})", ids)
            cur.execute(f"delete from scheduler_task_dependency where task_id in ({placeholders}) or depends_on_task_id in ({placeholders})", ids + ids)
            cur.execute(f"delete from scheduler_task where id in ({placeholders})", ids)
        cur.execute("delete from demo_biz_task where biz_key like %s", (RUN_ID + "%",))


def clear_redis(r: MiniRedis):
    base_keys = [
        f"sched:queue:time:{GROUP_CODE}",
        f"sched:queue:ready:{GROUP_CODE}",
        f"sched:active-users:{GROUP_CODE}",
        f"sched:ready:user:{GROUP_CODE}:{USER_ID}",
        f"sched:group:running:{GROUP_CODE}",
        f"sched:user:running:{GROUP_CODE}:{USER_ID}",
        f"sched:reconcile:throttle:{GROUP_CODE}",
    ]
    r.delete(*base_keys)
    for key in r.keys(f"sched:active-user-lock:{GROUP_CODE}:*"):
        r.delete(key)
    for key in r.keys("sched:task:lease:*"):
        r.delete(key)


def resolve_java_home() -> str:
    java_home_cmd = "/usr/libexec/java_home"
    if os.path.exists(java_home_cmd):
        resolved = subprocess.check_output([java_home_cmd, "-v", "21"], text=True).strip()
        if resolved:
            return resolved
    if os.environ.get("JAVA_HOME"):
        return os.environ["JAVA_HOME"]
    return "/Users/chenmingdong01/Library/Java/JavaVirtualMachines/openjdk-21.0.1/Contents/Home"


def build_demo():
    env = os.environ.copy()
    java_home = resolve_java_home()
    env["JAVA_HOME"] = java_home
    env["PATH"] = f"{java_home}/bin:" + env["PATH"]
    subprocess.run(
        ["mvn", "-q", "-pl", "demo-consumer", "-am", "-DskipTests", "package"],
        cwd=REPO_ROOT,
        env=env,
        check=True,
    )
    jars = [p for p in glob.glob(str(REPO_ROOT / "demo-consumer" / "target" / "*.jar")) if not p.endswith(".original")]
    if not jars:
        raise RuntimeError("demo-consumer jar not found")
    return jars[0], java_home


def start_app(jar_path: str, java_home: str, instance_id: str, log_path: Path):
    env = os.environ.copy()
    env["DEMO_DB_URL"] = os.environ["MYSQL_URL"]
    env["DEMO_DB_USERNAME"] = os.environ["MYSQL_USERNAME"]
    env["DEMO_DB_PASSWORD"] = os.environ["MYSQL_PASSWORD"]
    env["SPRING_DATA_REDIS_HOST"] = os.environ["REDIS_HOST"]
    env["SPRING_DATA_REDIS_PORT"] = os.environ["REDIS_PORT"]
    env["JAVA_HOME"] = java_home
    env["PATH"] = f"{java_home}/bin:" + env["PATH"]
    cmd = [
        f"{java_home}/bin/java",
        "-jar",
        jar_path,
        f"--server.port={DEMO_PORT}",
        "--utask.scheduler.dispatch-enabled=true",
        f"--utask.scheduler.instance-id={instance_id}",
        "--utask.scheduler.dispatch-interval-ms=300",
        "--utask.scheduler.recovery-interval-ms=1000",
        "--utask.scheduler.queue-refill-interval-ms=1000",
        f"--utask.scheduler.wait-hold-default-delay-sec={HOLD_RETRY_DELAY_SEC}",
        "--utask.scheduler.wait-hold-max-rounds=1000",
    ]
    log_file = open(log_path, "w")
    proc = subprocess.Popen(cmd, cwd=REPO_ROOT, env=env, stdout=log_file, stderr=subprocess.STDOUT)
    wait_http_ready()
    return proc, log_file


def wait_http_ready():
    deadline = time.time() + 30
    url = f"http://127.0.0.1:{DEMO_PORT}/demo/biz/health-check"
    while time.time() < deadline:
        try:
            requests.get(url, timeout=1)
            return
        except Exception:
            time.sleep(0.5)
    raise RuntimeError("demo-consumer did not become ready in time")


def stop_app(proc: subprocess.Popen, log_file):
    if proc.poll() is None:
        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=15)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)
    log_file.close()


def task_defs():
    execute_at = (datetime.now() + timedelta(seconds=2)).replace(microsecond=0).isoformat()
    defs = []
    for i in range(HOLD_TASK_COUNT):
        defs.append({
            "kind": "WAIT_HOLD",
            "bizKey": f"{RUN_ID}-hold-{i}",
            "priority": HOLD_PRIORITY,
            "holdMaxRounds": 5,
            "holdRetryDelaySec": HOLD_RETRY_DELAY_SEC,
            "extInfo": json.dumps({
                "failBeforeSuccess": 0,
                "waitHoldRoundsBeforeSuccess": 1,
                "sleepMs": HOLD_SUCCESS_SLEEP_MS,
            }),
            "executeAt": execute_at,
        })
    for i in range(NORMAL_TASK_COUNT):
        defs.append({
            "kind": "NORMAL",
            "bizKey": f"{RUN_ID}-normal-{i}",
            "priority": NORMAL_PRIORITY,
            "holdMaxRounds": 0,
            "holdRetryDelaySec": HOLD_RETRY_DELAY_SEC,
            "extInfo": json.dumps({
                "failBeforeSuccess": 0,
                "waitHoldRoundsBeforeSuccess": 0,
                "sleepMs": NORMAL_SLEEP_MS,
            }),
            "executeAt": execute_at,
        })
    return defs


def submit_one(task_def: dict[str, Any]):
    url = f"http://127.0.0.1:{DEMO_PORT}/demo/submit"
    payload = {
        "groupCode": GROUP_CODE,
        "userId": USER_ID,
        "bizKey": task_def["bizKey"],
        "priority": task_def["priority"],
        "maxRetryCount": 0,
        "holdMaxRounds": task_def["holdMaxRounds"],
        "holdRetryDelaySec": task_def["holdRetryDelaySec"],
        "executeTimeoutSec": 20,
        "executeAt": task_def["executeAt"],
        "extInfo": task_def["extInfo"],
        "payload": "{}",
    }
    resp = requests.post(url, json=payload, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    data["kind"] = task_def["kind"]
    data["priority"] = task_def["priority"]
    return data


def query_tasks(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            select id, biz_key, status, priority, hold_round_count, hold_max_rounds, hold_retry_delay_sec,
                   execute_at, dispatcher_instance, worker_instance, worker_thread, ext_info
              from scheduler_task
             where biz_key like %s
             order by id asc
            """,
            (RUN_ID + "%",),
        )
        rows = cur.fetchall()
        for row in rows:
            if row["execute_at"]:
                row["execute_at"] = row["execute_at"].isoformat(sep=" ")
        return rows


def query_executions(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            select t.biz_key, e.execute_no, e.status, e.dispatcher_instance, e.worker_instance, e.start_time, e.finish_time
              from scheduler_task_execution e
              join scheduler_task t on t.id = e.task_id
             where t.biz_key like %s
             order by e.start_time asc, e.id asc
            """,
            (RUN_ID + "%",),
        )
        rows = cur.fetchall()
        for row in rows:
            for col in ("start_time", "finish_time"):
                if row[col]:
                    row[col] = row[col].isoformat(sep=" ")
        return rows


def snapshot(conn, r: MiniRedis, label: str):
    return {
        "label": label,
        "tasks": query_tasks(conn),
        "executions": query_executions(conn),
        "redis": {
            "group_running": r.get(f"sched:group:running:{GROUP_CODE}"),
            "user_running": r.get(f"sched:user:running:{GROUP_CODE}:{USER_ID}"),
            "active_users": r.command("ZRANGE", f"sched:active-users:{GROUP_CODE}", "0", "-1", "WITHSCORES"),
            "ready_queue": r.command("ZRANGE", f"sched:ready:user:{GROUP_CODE}:{USER_ID}", "0", "-1", "WITHSCORES"),
            "time_queue": r.command("ZRANGE", f"sched:queue:time:{GROUP_CODE}", "0", "-1", "WITHSCORES"),
        },
    }


def wait_until(predicate, timeout_sec: int, interval_sec: float = 0.2, fail_message: str = "condition not met"):
    deadline = time.time() + timeout_sec
    last_value = None
    while time.time() < deadline:
        last_value = predicate()
        if last_value:
            return last_value
        time.sleep(interval_sec)
    raise RuntimeError(f"{fail_message}, last={last_value}")


def validate_during_hold(conn, r: MiniRedis):
    tasks = query_tasks(conn)
    executions = query_executions(conn)
    hold_task = next((t for t in tasks if "-hold-" in t["biz_key"]), None)
    if hold_task is None:
        raise RuntimeError("hold task not found")
    if hold_task["status"] != "WAIT_HOLD":
        return False
    if str(r.get(f"sched:group:running:{GROUP_CODE}")) != "1":
        return False
    if str(r.get(f"sched:user:running:{GROUP_CODE}:{USER_ID}")) != "1":
        return False
    non_hold_executions = [row for row in executions if "-normal-" in row["biz_key"]]
    if non_hold_executions:
        raise RuntimeError(f"normal tasks started while hold task occupied concurrency: {non_hold_executions}")
    return {"tasks": tasks, "executions": executions}


def wait_all_success(conn):
    tasks = query_tasks(conn)
    if len(tasks) != HOLD_TASK_COUNT + NORMAL_TASK_COUNT:
        return False
    if any(task["status"] != "SUCCESS" for task in tasks):
        return False
    return tasks


def validate_final_state(conn, r: MiniRedis):
    tasks = query_tasks(conn)
    executions = query_executions(conn)
    if any(task["status"] != "SUCCESS" for task in tasks):
        raise RuntimeError(f"not all tasks succeeded: {tasks}")
    if r.get(f"sched:group:running:{GROUP_CODE}") is not None:
        raise RuntimeError("group_running not released")
    if r.get(f"sched:user:running:{GROUP_CODE}:{USER_ID}") is not None:
        raise RuntimeError("user_running not released")
    if r.command("ZRANGE", f"sched:active-users:{GROUP_CODE}", "0", "-1", "WITHSCORES"):
        raise RuntimeError("active users queue not empty")
    if r.command("ZRANGE", f"sched:ready:user:{GROUP_CODE}:{USER_ID}", "0", "-1", "WITHSCORES"):
        raise RuntimeError("ready queue not empty")
    if r.command("ZRANGE", f"sched:queue:time:{GROUP_CODE}", "0", "-1", "WITHSCORES"):
        raise RuntimeError("time queue not empty")

    hold_execs = [row for row in executions if "-hold-" in row["biz_key"]]
    normal_execs = [row for row in executions if "-normal-" in row["biz_key"]]
    if not hold_execs or not normal_execs:
        raise RuntimeError("missing expected execution records")

    first_hold_success = next((row for row in hold_execs if row["status"] == "SUCCESS"), None)
    if first_hold_success is None:
        raise RuntimeError("hold task missing SUCCESS execution")
    hold_finish = first_hold_success["finish_time"]
    started_early = [row for row in normal_execs if row["start_time"] and row["start_time"] < hold_finish]
    if started_early:
        raise RuntimeError(f"normal tasks started before hold task released concurrency: {started_early}")
    return {
        "tasks": tasks,
        "executions": executions,
    }


def try_validate_final_state(conn, r: MiniRedis):
    try:
        return validate_final_state(conn, r)
    except Exception:
        return False


def main():
    ensure_runtime_env()
    db_cfg = parse_mysql_jdbc(os.environ["MYSQL_URL"])
    conn = db_conn(db_cfg)
    redis_cli = MiniRedis(os.environ["REDIS_HOST"], int(os.environ["REDIS_PORT"]))

    ensure_schema(conn)
    cleanup(conn)
    clear_redis(redis_cli)

    jar_path, java_home = build_demo()
    proc = log_file = None
    try:
        proc, log_file = start_app(jar_path, java_home, INSTANCE_ID, APP_LOG)
        defs = task_defs()
        with ThreadPoolExecutor(max_workers=len(defs)) as executor:
            submitted = list(executor.map(submit_one, defs))
        print(json.dumps({"submitted": submitted}, ensure_ascii=False, indent=2))

        submitted_snapshot = snapshot(conn, redis_cli, "after_concurrent_submit")
        print(json.dumps(submitted_snapshot, ensure_ascii=False, indent=2))

        during_hold = wait_until(
            lambda: validate_during_hold(conn, redis_cli),
            timeout_sec=20,
            fail_message="mixed tasks never reached expected WAIT_HOLD occupancy",
        )
        print(json.dumps({"label": "during_wait_hold", **during_hold}, ensure_ascii=False, indent=2))

        wait_until(lambda: wait_all_success(conn), timeout_sec=60, fail_message="tasks did not all succeed")
        final = wait_until(
            lambda: try_validate_final_state(conn, redis_cli),
            timeout_sec=10,
            fail_message="final mysql/redis state did not settle",
        )
        print(json.dumps({"label": "after_all_success", **final}, ensure_ascii=False, indent=2))

        print("\nSummary:")
        print(f"- run_id: {RUN_ID}")
        print(f"- app log: {APP_LOG}")
    finally:
        if proc and log_file:
            stop_app(proc, log_file)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"[error] {exc}", file=sys.stderr)
        sys.exit(1)
