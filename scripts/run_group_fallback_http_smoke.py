#!/usr/bin/env python3
"""Verify normal scheduling and Group fallback through a running demo-consumer HTTP service."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import uuid
from datetime import datetime, timedelta
from typing import Any
from urllib.parse import urlparse

import pymysql
import requests


DEFAULT_ENV_FILE = "/Users/chenmingdong01/Documents/secret/test.env"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://127.0.0.1:8099")
    parser.add_argument("--dispatch-route", default="",
                        help="Must match utask.scheduler.dispatch-route; leave blank for legacy route")
    parser.add_argument("--env-file", default=os.environ.get("ENV_FILE", DEFAULT_ENV_FILE))
    parser.add_argument("--fallback-delay-sec", type=int, default=3)
    parser.add_argument("--execute-delay-sec", type=int, default=20)
    parser.add_argument("--timeout-sec", type=int, default=45)
    parser.add_argument("--keep-data", action="store_true")
    return parser.parse_args()


def load_env_file(path: str) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path or not os.path.exists(path):
        return values
    with open(path, encoding="utf-8") as file:
        for line in file:
            text = line.strip()
            if text and not text.startswith("#") and "=" in text:
                key, value = text.split("=", 1)
                values[key.strip()] = value.strip()
    return values


def required_config(args: argparse.Namespace) -> dict[str, str]:
    file_values = load_env_file(args.env_file)
    aliases = {
        "MYSQL_URL": ("MYSQL_URL", "DEMO_DB_URL"),
        "MYSQL_USERNAME": ("MYSQL_USERNAME", "DEMO_DB_USERNAME"),
        "MYSQL_PASSWORD": ("MYSQL_PASSWORD", "DEMO_DB_PASSWORD"),
        "REDIS_HOST": ("REDIS_HOST", "SPRING_DATA_REDIS_HOST"),
        "REDIS_PORT": ("REDIS_PORT", "SPRING_DATA_REDIS_PORT"),
    }
    result: dict[str, str] = {}
    for target, names in aliases.items():
        value = next((os.environ.get(name) or file_values.get(name) for name in names
                      if os.environ.get(name) or file_values.get(name)), None)
        if value is None:
            raise RuntimeError(f"missing {target}; set it in environment or {args.env_file}")
        result[target] = value
    return result


def mysql_connection(config: dict[str, str]):
    jdbc_url = config["MYSQL_URL"]
    if not jdbc_url.startswith("jdbc:mysql://"):
        raise RuntimeError(f"unsupported MYSQL_URL: {jdbc_url}")
    parsed = urlparse(jdbc_url[len("jdbc:"):])
    database = parsed.path.lstrip("/")
    if not database:
        raise RuntimeError("database is missing from MYSQL_URL")
    return pymysql.connect(
        host=parsed.hostname or "127.0.0.1",
        port=parsed.port or 3306,
        user=config["MYSQL_USERNAME"],
        password=config["MYSQL_PASSWORD"],
        database=database,
        charset="utf8mb4",
        autocommit=True,
        cursorclass=pymysql.cursors.DictCursor,
    )


def ensure_group(conn, group_code: str, description: str) -> None:
    sql = """
        INSERT INTO scheduler_group_config(
            group_code, enabled, max_concurrency, user_base_concurrency,
            dynamic_user_limit_enabled, load_strategy_json,
            dispatch_batch_size, heartbeat_timeout_sec, lock_expire_sec, description
        ) VALUES (%s, 1, 10, 10, 0, NULL, 50, 30, 60, %s)
        ON DUPLICATE KEY UPDATE
            enabled=VALUES(enabled), max_concurrency=VALUES(max_concurrency),
            user_base_concurrency=VALUES(user_base_concurrency),
            dispatch_batch_size=VALUES(dispatch_batch_size), description=VALUES(description)
    """
    with conn.cursor() as cursor:
        cursor.execute(sql, (group_code, description))


def submit(base_url: str, body: dict[str, Any]) -> dict[str, Any]:
    response = requests.post(f"{base_url.rstrip('/')}/demo/submit", json=body, timeout=10)
    response.raise_for_status()
    data = response.json()
    if "taskId" not in data or "bizKey" not in data:
        raise RuntimeError(f"unexpected submit response: {data}")
    return data


def verify_service_config(base_url: str, route: str) -> None:
    response = requests.get(f"{base_url.rstrip('/')}/demo/scheduler-config", timeout=10)
    if response.status_code == 404:
        raise RuntimeError("service does not expose /demo/scheduler-config; rebuild and restart demo-consumer")
    response.raise_for_status()
    config = response.json()
    actual_route = str(config.get("dispatchRoute") or "").strip()
    if not config.get("dispatchEnabled"):
        raise RuntimeError("service has utask.scheduler.dispatch-enabled=false")
    if not config.get("fallbackEnabled"):
        raise RuntimeError("service has utask.scheduler.fallback-enabled=false")
    if actual_route != route:
        expected = route or "<blank>"
        actual = actual_route or "<blank>"
        raise RuntimeError(
            f"dispatch route mismatch: script={expected}, service={actual}; "
            "start the service with the same --utask.scheduler.dispatch-route"
        )


def task_by_id(conn, task_id: int) -> dict[str, Any] | None:
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT id, task_no, status, group_code, dispatch_route, retry_count,
                   fallback_policy_count, group_fallback_count, fallback_check_at,
                   execute_at, create_time, update_time, start_time, finish_time
              FROM scheduler_task WHERE id=%s
        """, (task_id,))
        return cursor.fetchone()


def business_status(conn, biz_key: str) -> str | None:
    with conn.cursor() as cursor:
        cursor.execute("SELECT status FROM demo_biz_task WHERE biz_key=%s", (biz_key,))
        row = cursor.fetchone()
        return None if row is None else row["status"]


def fallback_log_count(conn, task_id: int, source_group: str, target_group: str) -> int:
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT COUNT(1) AS count FROM scheduler_task_group_fallback_log
             WHERE task_id=%s AND source_group_code=%s AND target_group_code=%s
        """, (task_id, source_group, target_group))
        return int(cursor.fetchone()["count"])


def wait_until(timeout_sec: int, description: str, condition):
    deadline = time.monotonic() + timeout_sec
    while time.monotonic() < deadline:
        value = condition()
        if value:
            return value
        time.sleep(0.25)
    raise AssertionError(f"timeout waiting for {description}")


def cleanup(conn, run_prefix: str, groups: list[str]) -> None:
    with conn.cursor() as cursor:
        cursor.execute("SELECT id FROM scheduler_task WHERE biz_key LIKE %s", (run_prefix + "%",))
        task_ids = [row["id"] for row in cursor.fetchall()]
        if task_ids:
            placeholders = ",".join(["%s"] * len(task_ids))
            cursor.execute(f"DELETE FROM scheduler_task_group_fallback_log WHERE task_id IN ({placeholders})", task_ids)
            cursor.execute(f"DELETE FROM scheduler_task_execution WHERE task_id IN ({placeholders})", task_ids)
            cursor.execute(
                f"DELETE FROM scheduler_task_dependency WHERE task_id IN ({placeholders}) "
                f"OR depends_on_task_id IN ({placeholders})", task_ids + task_ids)
            cursor.execute(f"DELETE FROM scheduler_task WHERE id IN ({placeholders})", task_ids)
        cursor.execute("DELETE FROM demo_biz_task WHERE biz_key LIKE %s", (run_prefix + "%",))
        cursor.execute("DELETE FROM scheduler_group_config WHERE group_code IN (%s, %s)", groups)


def main() -> int:
    args = parse_args()
    if args.fallback_delay_sec < 1 or args.execute_delay_sec < args.fallback_delay_sec + 5:
        raise SystemExit(
            "execute-delay-sec must be at least five seconds later than fallback-delay-sec, "
            "and fallback-delay-sec must be >= 1"
        )
    config = required_config(args)
    run_id = uuid.uuid4().hex[:12]
    run_prefix = f"fallback-http-smoke-{run_id}-"
    source_group = f"fallback-smoke-source-{run_id}"
    target_group = f"fallback-smoke-target-{run_id}"
    route = args.dispatch_route.strip()
    conn = mysql_connection(config)
    try:
        verify_service_config(args.base_url, route)
        ensure_group(conn, source_group, "HTTP fallback smoke source")
        ensure_group(conn, target_group, "HTTP fallback smoke target")
        now = datetime.now()
        common = {"userId": f"fallback-smoke-user-{run_id}", "maxRetryCount": 0}
        if route:
            common["dispatchRoute"] = route

        normal = submit(args.base_url, {
            **common,
            "groupCode": source_group,
            "bizKey": run_prefix + "normal",
            "executeAt": now.isoformat(timespec="seconds"),
            "extInfo": json.dumps({"failBeforeSuccess": 0, "sleepMs": 0}),
        })
        normal_id = int(normal["taskId"])
        try:
            normal_task = wait_until(args.timeout_sec, "normal task success", lambda: (
                row if (row := task_by_id(conn, normal_id)) and row["status"] == "SUCCESS" else None))
        except AssertionError as error:
            snapshot = task_by_id(conn, normal_id)
            raise AssertionError(f"{error}; task snapshot={snapshot}") from error
        if normal_task["group_code"] != source_group or normal_task["group_fallback_count"] != 0:
            raise AssertionError(f"normal task changed unexpectedly: {normal_task}")
        if business_status(conn, normal["bizKey"]) != "SUCCESS":
            raise AssertionError("normal demo business task was not successful")
        print(f"PASS normal scheduling: taskId={normal['taskId']} status=SUCCESS")

        # Create timing relative to this point, rather than relative to the normal
        # task submission. This leaves the fallback scanner a real, safe window.
        fallback_now = datetime.now()
        fallback = submit(args.base_url, {
            **common,
            "groupCode": source_group,
            "bizKey": run_prefix + "fallback",
            "executeAt": (
                fallback_now + timedelta(seconds=args.execute_delay_sec)
            ).isoformat(timespec="seconds"),
            "fallbackCheckAt": (
                fallback_now + timedelta(seconds=args.fallback_delay_sec)
            ).isoformat(timespec="seconds"),
            "extInfo": json.dumps({
                "failBeforeSuccess": 0,
                "sleepMs": 0,
                "fallbackTargetGroup": target_group,
            }),
        })
        fallback_id = int(fallback["taskId"])
        initial = task_by_id(conn, fallback_id)
        if initial is None or initial["status"] != "PENDING" or initial["group_code"] != source_group:
            raise AssertionError(f"fallback task was not pending in its source group: {initial}")
        try:
            routed = wait_until(args.timeout_sec, "fallback group route", lambda: (
                row if (row := task_by_id(conn, fallback_id)) and row["group_code"] == target_group else None))
        except AssertionError as error:
            snapshot = task_by_id(conn, fallback_id)
            logs = fallback_log_count(conn, fallback_id, source_group, target_group)
            raise AssertionError(f"{error}; task snapshot={snapshot}; fallback audit logs={logs}") from error
        if routed["group_fallback_count"] != 1 or routed["fallback_policy_count"] != 1:
            raise AssertionError(f"unexpected fallback counters: {routed}")
        if fallback_log_count(conn, fallback_id, source_group, target_group) != 1:
            raise AssertionError("expected exactly one fallback audit log")
        print(f"PASS group fallback: taskId={fallback_id} {source_group} -> {target_group}")

        completed = wait_until(args.timeout_sec, "routed task success", lambda: (
            row if (row := task_by_id(conn, fallback_id)) and row["status"] == "SUCCESS" else None))
        if completed["group_code"] != target_group:
            raise AssertionError(f"routed task executed in unexpected group: {completed}")
        if business_status(conn, fallback["bizKey"]) != "SUCCESS":
            raise AssertionError("routed demo business task was not successful")
        print(f"PASS routed task execution: taskId={fallback_id} status=SUCCESS")

        execution_route = submit(args.base_url, {
            **common,
            "groupCode": source_group,
            "maxRetryCount": 1,
            "bizKey": run_prefix + "execution-route",
            "executeAt": datetime.now().isoformat(timespec="seconds"),
            "extInfo": json.dumps({
                "failBeforeSuccess": 1,
                "sleepMs": 0,
                "executeRetryTargetGroup": target_group,
            }),
        })
        execution_route_id = int(execution_route["taskId"])
        try:
            execution_routed = wait_until(args.timeout_sec, "execution-result group route", lambda: (
                row if (row := task_by_id(conn, execution_route_id))
                and row["group_code"] == target_group else None))
        except AssertionError as error:
            snapshot = task_by_id(conn, execution_route_id)
            raise AssertionError(f"{error}; task snapshot={snapshot}") from error
        if execution_routed["group_fallback_count"] != 0 or execution_routed["fallback_policy_count"] != 0:
            raise AssertionError(f"execution-result route used fallback state unexpectedly: {execution_routed}")
        if fallback_log_count(conn, execution_route_id, source_group, target_group) != 0:
            raise AssertionError("execution-result route unexpectedly wrote a fallback audit log")
        execution_completed = wait_until(args.timeout_sec, "execution-result routed task success", lambda: (
            row if (row := task_by_id(conn, execution_route_id)) and row["status"] == "SUCCESS" else None))
        if execution_completed["group_code"] != target_group:
            raise AssertionError(f"execution-result task completed in unexpected group: {execution_completed}")
        if business_status(conn, execution_route["bizKey"]) != "SUCCESS":
            raise AssertionError("execution-result routed demo business task was not successful")
        print(f"PASS execution-result group route: taskId={execution_route_id} status=SUCCESS")
        print("SMOKE TEST PASSED")
        return 0
    finally:
        if not args.keep_data:
            cleanup(conn, run_prefix, [source_group, target_group])
            print("cleaned dedicated smoke-test data")
        conn.close()


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as error:
        print(f"SMOKE TEST FAILED: {error}", file=sys.stderr)
        sys.exit(1)
