#!/usr/bin/env python3
"""Concurrent submit load test for demo-consumer instances.

Usage example:
python3 scripts/concurrent_submit_test.py \
  --base-urls http://127.0.0.1:8088,http://127.0.0.1:8089,http://127.0.0.1:8090 \
  --total 300 --concurrency 60 --users 30 --poll-status
"""

from __future__ import annotations

import argparse
import json
import random
import string
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen


def random_suffix(length: int = 8) -> str:
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=length))


def http_post_json(url: str, payload: dict, timeout: float = 5.0) -> tuple[int, dict | str]:
    data = json.dumps(payload).encode("utf-8")
    req = Request(url, data=data, headers={"Content-Type": "application/json"}, method="POST")
    with urlopen(req, timeout=timeout) as resp:
        body = resp.read().decode("utf-8")
        return resp.getcode(), json.loads(body)


def http_get_json(url: str, timeout: float = 5.0) -> tuple[int, dict | str]:
    req = Request(url, method="GET")
    with urlopen(req, timeout=timeout) as resp:
        body = resp.read().decode("utf-8")
        return resp.getcode(), json.loads(body) if body else {}


def submit_one(base_url: str, idx: int, args, start_ts: float) -> dict:
    user_id = f"u-{idx % args.users:03d}"
    biz_key = f"load-{int(start_ts)}-{idx:06d}-{random_suffix(4)}"
    payload = {
        "groupCode": args.group_code,
        "userId": user_id,
        "bizKey": biz_key,
        "priority": random.randint(args.min_priority, args.max_priority),
        "executeTimeoutSec": args.execute_timeout_sec,
        "retryDelaySec": args.retry_delay_sec,
        "payload": f"{{\"trace\":\"{biz_key}\"}}",
    }
    if args.force_retry_ratio > 0 and random.random() < args.force_retry_ratio:
        payload["forceRetry"] = True

    submit_url = f"{base_url.rstrip('/')}/demo/submit"

    try:
        code, resp = http_post_json(submit_url, payload, timeout=args.timeout)
        if code != 200:
            return {"ok": False, "error": f"HTTP_{code}", "base_url": base_url}
        if not isinstance(resp, dict) or "bizKey" not in resp:
            return {"ok": False, "error": "INVALID_RESPONSE", "base_url": base_url}

        result = {
            "ok": True,
            "base_url": base_url,
            "biz_key": str(resp["bizKey"]),
            "task_id": resp.get("taskId"),
        }

        if args.poll_status:
            status_url = f"{base_url.rstrip('/')}/demo/biz/{quote(result['biz_key'])}"
            deadline = time.time() + args.poll_timeout
            last_status = "UNKNOWN"
            while time.time() < deadline:
                try:
                    _, biz = http_get_json(status_url, timeout=args.timeout)
                    if isinstance(biz, dict):
                        last_status = str(biz.get("status", "UNKNOWN"))
                        if last_status in {"SUCCESS", "FAILED"}:
                            break
                except Exception:
                    pass
                time.sleep(args.poll_interval)
            result["final_status"] = last_status

        return result
    except HTTPError as e:
        return {"ok": False, "error": f"HTTP_{e.code}", "base_url": base_url}
    except URLError as e:
        return {"ok": False, "error": f"URL_ERROR:{e.reason}", "base_url": base_url}
    except Exception as e:
        return {"ok": False, "error": f"EXCEPTION:{e}", "base_url": base_url}


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Concurrent submit load test for demo-consumer")
    p.add_argument(
        "--base-urls",
        default="http://127.0.0.1:8088,http://127.0.0.1:8089,http://127.0.0.1:8090",
        help="Comma-separated base URLs of instances",
    )
    p.add_argument("--total", type=int, default=200, help="Total submit requests")
    p.add_argument("--concurrency", type=int, default=50, help="Thread pool size")
    p.add_argument("--users", type=int, default=20, help="Number of logical users to distribute")
    p.add_argument("--group-code", default="demo-group", help="groupCode in submit payload")
    p.add_argument("--min-priority", type=int, default=10)
    p.add_argument("--max-priority", type=int, default=99)
    p.add_argument(
        "--execute-timeout-sec",
        "--execute_timeout_sec",
        dest="execute_timeout_sec",
        type=int,
        default=30,
    )
    p.add_argument("--retry-delay-sec", type=int, default=5)
    p.add_argument("--force-retry-ratio", type=float, default=0.0, help="0.0~1.0")
    p.add_argument("--timeout", type=float, default=5.0, help="HTTP timeout (seconds)")
    p.add_argument("--poll-status", action="store_true", help="Poll /demo/biz/{bizKey} after submit")
    p.add_argument("--poll-timeout", type=float, default=20.0, help="Max poll duration per request")
    p.add_argument("--poll-interval", type=float, default=0.5, help="Poll interval")
    return p


def main() -> None:
    args = build_parser().parse_args()
    base_urls = [x.strip() for x in args.base_urls.split(",") if x.strip()]
    if not base_urls:
        raise SystemExit("No base URLs provided")
    if args.total <= 0 or args.concurrency <= 0:
        raise SystemExit("--total and --concurrency must be > 0")

    start = time.time()
    lock = threading.Lock()
    rr = {"i": 0}

    def pick_url() -> str:
        with lock:
            u = base_urls[rr["i"] % len(base_urls)]
            rr["i"] += 1
            return u

    futures = []
    results = []

    with ThreadPoolExecutor(max_workers=args.concurrency) as ex:
        for i in range(args.total):
            url = pick_url()
            futures.append(ex.submit(submit_one, url, i, args, start))

        for f in as_completed(futures):
            results.append(f.result())

    elapsed = time.time() - start
    ok_count = sum(1 for r in results if r.get("ok"))
    fail_count = len(results) - ok_count

    by_instance = Counter(r.get("base_url", "UNKNOWN") for r in results)
    fail_reason = Counter(r.get("error", "UNKNOWN") for r in results if not r.get("ok"))
    final_status = Counter(r.get("final_status", "NO_POLL") for r in results if r.get("ok"))

    print("=" * 72)
    print(f"Total Requests : {args.total}")
    print(f"Concurrency    : {args.concurrency}")
    print(f"Instances      : {', '.join(base_urls)}")
    print(f"Elapsed(s)     : {elapsed:.2f}")
    print(f"QPS            : {args.total / elapsed:.2f}")
    print(f"Success        : {ok_count}")
    print(f"Failed         : {fail_count}")
    print("-" * 72)
    print("Requests by instance:")
    for k, v in sorted(by_instance.items()):
        print(f"  {k:<30} {v}")

    if fail_reason:
        print("-" * 72)
        print("Failure reasons:")
        for k, v in fail_reason.most_common():
            print(f"  {k:<40} {v}")

    if args.poll_status:
        print("-" * 72)
        print("Final status distribution:")
        for k, v in final_status.most_common():
            print(f"  {k:<20} {v}")


if __name__ == "__main__":
    main()
