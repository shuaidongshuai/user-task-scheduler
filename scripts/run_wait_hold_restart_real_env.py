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
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

import pymysql
import requests


DEFAULT_ENV_FILE = "/Users/chenmingdong01/Documents/密钥/test.env"
REPO_ROOT = Path(__file__).resolve().parents[1]
DEMO_PORT = int(os.environ.get("WAIT_HOLD_DEMO_PORT", "18099"))
GROUP_CODE = os.environ.get("WAIT_HOLD_GROUP", "codex_wait_hold_real")
USER_ID = os.environ.get("WAIT_HOLD_USER", "codex-user")
RUN_ID = os.environ.get("WAIT_HOLD_RUN_ID", "codex-wait-hold-" + uuid.uuid4().hex[:10])
INSTANCE_1 = os.environ.get("WAIT_HOLD_INSTANCE1", "codex-wait-hold-a")
INSTANCE_2 = os.environ.get("WAIT_HOLD_INSTANCE2", "codex-wait-hold-b")
APP_LOG_1 = REPO_ROOT / "tmp" / f"{RUN_ID}-app-1.log"
APP_LOG_2 = REPO_ROOT / "tmp" / f"{RUN_ID}-app-2.log"
APP_LOG_1.parent.mkdir(parents=True, exist_ok=True)


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
    database = parsed.path.lstrip("/")
    return DbConfig(
        host=parsed.hostname or "127.0.0.1",
        port=parsed.port or 3306,
        database=database,
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
            ) VALUES (%s,1,1,1,0,NULL,20,30,60,'wait hold real env test')
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
            (GROUP_CODE,),
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
        "--utask.scheduler.wait-hold-default-delay-sec=2",
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


def submit_task():
    url = f"http://127.0.0.1:{DEMO_PORT}/demo/submit"
    ext = {
        "failBeforeSuccess": 0,
        "waitHoldRoundsBeforeSuccess": 1,
        "sleepMs": 100,
    }
    payload = {
        "groupCode": GROUP_CODE,
        "userId": USER_ID,
        "bizKey": RUN_ID + "-biz",
        "priority": 10,
        "maxRetryCount": 0,
        "holdMaxRounds": 5,
        "holdRetryDelaySec": 2,
        "executeTimeoutSec": 10,
        "executeAt": time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(time.time() - 1)),
        "extInfo": json.dumps(ext),
        "payload": "{}",
    }
    resp = requests.post(url, json=payload, timeout=10)
    resp.raise_for_status()
    return resp.json()


def query_task(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            select id, task_no, status, hold_round_count, hold_max_rounds, hold_retry_delay_sec,
                   execute_at, ext_info, dispatcher_instance, worker_instance, worker_thread
              from scheduler_task
             where biz_key = %s
            """,
            (RUN_ID + "-biz",),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("scheduler_task row not found")
        row["execute_at"] = row["execute_at"].isoformat(sep=" ") if row["execute_at"] else None
        return row


def query_execution_rows(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            select execute_no, status, dispatcher_instance, worker_instance, start_time, finish_time, error_code
              from scheduler_task_execution
             where task_id = %s
             order by id asc
            """,
            (query_task(conn)["id"],),
        )
        rows = cur.fetchall()
        for row in rows:
            for col in ("start_time", "finish_time"):
                if row[col]:
                    row[col] = row[col].isoformat(sep=" ")
        return rows


def snapshot(conn, r: MiniRedis, label: str):
    task = query_task(conn)
    task_id = task["id"]
    result = {
        "label": label,
        "db": task,
        "executions": query_execution_rows(conn),
        "redis": {
            "time_queue": r.zscore(f"sched:queue:time:{GROUP_CODE}", str(task_id)),
            "user_ready_queue": r.zscore(f"sched:ready:user:{GROUP_CODE}:{USER_ID}", str(task_id)),
            "active_users": r.zscore(f"sched:active-users:{GROUP_CODE}", USER_ID),
            "group_running": r.get(f"sched:group:running:{GROUP_CODE}"),
            "user_running": r.get(f"sched:user:running:{GROUP_CODE}:{USER_ID}"),
            "task_lease": r.get(f"sched:task:lease:{task_id}"),
        },
    }
    return result


def wait_for_status(conn, target: str, timeout_sec: int):
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        row = query_task(conn)
        if row["status"] == target:
            return row
        time.sleep(0.2)
    raise RuntimeError(f"task did not reach status={target}, current={query_task(conn)['status']}")


def main():
    ensure_runtime_env()

    db_cfg = parse_mysql_jdbc(os.environ["MYSQL_URL"])
    conn = db_conn(db_cfg)
    redis_cli = MiniRedis(os.environ["REDIS_HOST"], int(os.environ["REDIS_PORT"]))

    ensure_schema(conn)
    cleanup(conn)
    clear_redis(redis_cli)

    jar_path, java_home = build_demo()
    proc1 = log1 = None
    proc2 = log2 = None
    try:
        proc1, log1 = start_app(jar_path, java_home, INSTANCE_1, APP_LOG_1)
        submit_result = submit_task()
        print(json.dumps({"submit": submit_result}, ensure_ascii=False, indent=2))

        submitted = snapshot(conn, redis_cli, "after_submit")
        print(json.dumps(submitted, ensure_ascii=False, indent=2))

        wait_for_status(conn, "WAIT_HOLD", 20)
        waiting = snapshot(conn, redis_cli, "during_wait_hold_before_restart")
        print(json.dumps(waiting, ensure_ascii=False, indent=2))

        stop_app(proc1, log1)
        proc1 = None
        log1 = None
        time.sleep(3)

        proc2, log2 = start_app(jar_path, java_home, INSTANCE_2, APP_LOG_2)
        wait_for_status(conn, "SUCCESS", 20)
        finished = snapshot(conn, redis_cli, "after_restart_and_success")
        print(json.dumps(finished, ensure_ascii=False, indent=2))

        print("\nSummary:")
        print(f"- run_id: {RUN_ID}")
        print(f"- app log #1: {APP_LOG_1}")
        print(f"- app log #2: {APP_LOG_2}")
    finally:
        if proc1 and log1:
            stop_app(proc1, log1)
        if proc2 and log2:
            stop_app(proc2, log2)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"[error] {exc}", file=sys.stderr)
        sys.exit(1)
