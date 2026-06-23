#!/usr/bin/env python3
import argparse
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
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import pymysql
import requests


DEFAULT_ENV_FILE = "/Users/chenmingdong01/Documents/密钥/test.env"
REPO_ROOT = Path(__file__).resolve().parents[1]


def parse_args():
    parser = argparse.ArgumentParser(description="Single task real-env verifier")
    parser.add_argument("--mode", choices=["normal", "wait_hold"], required=True)
    parser.add_argument("--port", type=int, default=18119)
    return parser.parse_args()


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

    def zrange(self, key: str):
        return self.command("ZRANGE", key, "0", "-1", "WITHSCORES") or []

    def delete(self, *keys: str):
        if keys:
            self.command("DEL", *keys)

    def keys(self, pattern: str):
        return self.command("KEYS", pattern) or []

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


def resolve_java_home() -> str:
    java_home_cmd = "/usr/libexec/java_home"
    if os.path.exists(java_home_cmd):
        resolved = subprocess.check_output([java_home_cmd, "-v", "21"], text=True).strip()
        if resolved:
            return resolved
    if os.environ.get("JAVA_HOME"):
        return os.environ["JAVA_HOME"]
    return "/Users/chenmingdong01/Library/Java/JavaVirtualMachines/openjdk-21.0.1/Contents/Home"


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


def ensure_schema(conn, group_code: str):
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
            ) VALUES (%s,1,1,1,0,NULL,20,30,60,'single submit real env test')
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
            (group_code,),
        )


def cleanup(conn, run_id: str):
    with conn.cursor() as cur:
        cur.execute("select id from scheduler_task where biz_key like %s", (run_id + "%",))
        ids = [row["id"] for row in cur.fetchall()]
        if ids:
            placeholders = ",".join(["%s"] * len(ids))
            cur.execute(f"delete from scheduler_task_execution where task_id in ({placeholders})", ids)
            cur.execute(f"delete from scheduler_task_dependency where task_id in ({placeholders}) or depends_on_task_id in ({placeholders})", ids + ids)
            cur.execute(f"delete from scheduler_task where id in ({placeholders})", ids)
        cur.execute("delete from demo_biz_task where biz_key like %s", (run_id + "%",))


def clear_redis(r: MiniRedis, group_code: str, user_id: str):
    r.delete(
        f"sched:queue:time:{group_code}",
        f"sched:queue:ready:{group_code}",
        f"sched:active-users:{group_code}",
        f"sched:ready:user:{group_code}:{user_id}",
        f"sched:group:running:{group_code}",
        f"sched:user:running:{group_code}:{user_id}",
        f"sched:reconcile:throttle:{group_code}",
    )
    for key in r.keys(f"sched:active-user-lock:{group_code}:*"):
        r.delete(key)
    for key in r.keys("sched:task:lease:*"):
        r.delete(key)


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


def wait_http_ready(port: int):
    deadline = time.time() + 30
    url = f"http://127.0.0.1:{port}/demo/biz/health-check"
    while time.time() < deadline:
        try:
            requests.get(url, timeout=1)
            return
        except Exception:
            time.sleep(0.5)
    raise RuntimeError("demo-consumer did not become ready in time")


def start_app(jar_path: str, java_home: str, port: int, instance_id: str, log_path: Path):
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
        f"--server.port={port}",
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
    wait_http_ready(port)
    return proc, log_file


def stop_app(proc: subprocess.Popen, log_file):
    if proc.poll() is None:
        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=15)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)
    log_file.close()


def submit_task(port: int, group_code: str, user_id: str, biz_key: str, mode: str):
    ext = {
        "failBeforeSuccess": 0,
        "waitHoldRoundsBeforeSuccess": 1 if mode == "wait_hold" else 0,
        "sleepMs": 300,
    }
    payload = {
        "groupCode": group_code,
        "userId": user_id,
        "bizKey": biz_key,
        "priority": 0,
        "maxRetryCount": 0,
        "holdMaxRounds": 5 if mode == "wait_hold" else 0,
        "holdRetryDelaySec": 2,
        "executeTimeoutSec": 20,
        "executeAt": (datetime.now() + timedelta(seconds=1)).replace(microsecond=0).isoformat(),
        "extInfo": json.dumps(ext),
        "payload": "{}",
    }
    resp = requests.post(f"http://127.0.0.1:{port}/demo/submit", json=payload, timeout=15)
    resp.raise_for_status()
    return resp.json()


def query_task(conn, biz_key: str):
    with conn.cursor() as cur:
        cur.execute(
            """
            select id, biz_key, status, hold_round_count, hold_max_rounds, hold_retry_delay_sec,
                   execute_at, dispatcher_instance, worker_instance, worker_thread, ext_info
              from scheduler_task
             where biz_key = %s
            """,
            (biz_key,),
        )
        row = cur.fetchone()
        if row and row["execute_at"]:
            row["execute_at"] = row["execute_at"].isoformat(sep=" ")
        return row


def query_executions(conn, biz_key: str):
    with conn.cursor() as cur:
        cur.execute(
            """
            select e.execute_no, e.status, e.dispatcher_instance, e.worker_instance, e.start_time, e.finish_time
              from scheduler_task_execution e
              join scheduler_task t on t.id = e.task_id
             where t.biz_key = %s
             order by e.id
            """,
            (biz_key,),
        )
        rows = cur.fetchall()
        for row in rows:
            for col in ("start_time", "finish_time"):
                if row[col]:
                    row[col] = row[col].isoformat(sep=" ")
        return rows


def snapshot(conn, redis_cli: MiniRedis, group_code: str, user_id: str, biz_key: str, label: str):
    return {
        "label": label,
        "task": query_task(conn, biz_key),
        "executions": query_executions(conn, biz_key),
        "redis": {
            "group_running": redis_cli.get(f"sched:group:running:{group_code}"),
            "user_running": redis_cli.get(f"sched:user:running:{group_code}:{user_id}"),
            "active_users": redis_cli.zrange(f"sched:active-users:{group_code}"),
            "ready_queue": redis_cli.zrange(f"sched:ready:user:{group_code}:{user_id}"),
            "time_queue": redis_cli.zrange(f"sched:queue:time:{group_code}"),
        },
    }


def wait_until(predicate, timeout_sec: int, interval_sec: float = 0.2):
    deadline = time.time() + timeout_sec
    last = None
    while time.time() < deadline:
        last = predicate()
        if last:
            return last
        time.sleep(interval_sec)
    raise RuntimeError(f"condition not met, last={last}")


def main():
    args = parse_args()
    ensure_runtime_env()
    run_id = f"codex-single-{args.mode}-{uuid.uuid4().hex[:8]}"
    group_code = f"codex_single_{args.mode}_real"
    user_id = f"codex-single-{args.mode}-user"
    biz_key = run_id + "-biz"
    log_path = REPO_ROOT / "tmp" / f"{run_id}-app.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)

    db_cfg = parse_mysql_jdbc(os.environ["MYSQL_URL"])
    conn = db_conn(db_cfg)
    redis_cli = MiniRedis(os.environ["REDIS_HOST"], int(os.environ["REDIS_PORT"]))
    ensure_schema(conn, group_code)
    cleanup(conn, run_id)
    clear_redis(redis_cli, group_code, user_id)

    jar_path, java_home = build_demo()
    proc = log_file = None
    try:
        proc, log_file = start_app(jar_path, java_home, args.port, f"codex-single-{args.mode}", log_path)
        submit_result = submit_task(args.port, group_code, user_id, biz_key, args.mode)
        print(json.dumps({"submit": submit_result}, ensure_ascii=False, indent=2))
        print(json.dumps(snapshot(conn, redis_cli, group_code, user_id, biz_key, "after_submit"), ensure_ascii=False, indent=2))

        if args.mode == "wait_hold":
            wait_until(lambda: query_task(conn, biz_key)["status"] == "WAIT_HOLD", 20)
            print(json.dumps(snapshot(conn, redis_cli, group_code, user_id, biz_key, "during_wait_hold"), ensure_ascii=False, indent=2))

        wait_until(lambda: query_task(conn, biz_key)["status"] == "SUCCESS", 30)
        print(json.dumps(snapshot(conn, redis_cli, group_code, user_id, biz_key, "after_success"), ensure_ascii=False, indent=2))
        print("\nSummary:")
        print(f"- mode: {args.mode}")
        print(f"- run_id: {run_id}")
        print(f"- app log: {log_path}")
    finally:
        if proc and log_file:
            stop_app(proc, log_file)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"[error] {exc}", file=sys.stderr)
        sys.exit(1)
