#!/usr/bin/env python3
"""Start 3 local demo-consumer instances on different ports.

Example:
python3 scripts/start_three_instances.py \
  --java-home /path/to/jdk-21 \
  --db-url 'jdbc:mysql://127.0.0.1:3306/scheduler_demo?useUnicode=true&characterEncoding=utf8&serverTimezone=SYSTEM&allowPublicKeyRetrieval=true&useSSL=false' \
  --db-username root \
  --db-password root
"""

from __future__ import annotations

import argparse
import glob
import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Start 3 local demo-consumer instances")
    parser.add_argument(
        "--ports",
        default="8088,8089,8090",
        help="Comma-separated ports for instances",
    )
    parser.add_argument(
        "--java-home",
        default=os.environ.get("JAVA_HOME", ""),
        help="JAVA_HOME path (defaults to current JAVA_HOME)",
    )
    parser.add_argument(
        "--db-url",
        default=os.environ.get(
            "DEMO_DB_URL",
            "jdbc:mysql://127.0.0.1:3306/scheduler_demo?useUnicode=true&characterEncoding=utf8&serverTimezone=SYSTEM&allowPublicKeyRetrieval=true&useSSL=false",
        ),
        help="DEMO_DB_URL",
    )
    parser.add_argument("--db-username", default=os.environ.get("DEMO_DB_USERNAME", "root"))
    parser.add_argument("--db-password", default=os.environ.get("DEMO_DB_PASSWORD", "root"))
    parser.add_argument(
        "--build",
        action="store_true",
        help="Run maven package before starting (recommended)",
    )
    parser.add_argument(
        "--skip-build",
        action="store_true",
        help="Skip maven package (requires existing jar)",
    )
    return parser.parse_args()


def parse_ports(ports_text: str) -> list[int]:
    ports = []
    for p in ports_text.split(","):
        p = p.strip()
        if not p:
            continue
        try:
            v = int(p)
        except ValueError as exc:
            raise SystemExit(f"Invalid port: {p}") from exc
        if v <= 0 or v > 65535:
            raise SystemExit(f"Port out of range: {v}")
        ports.append(v)
    if len(ports) != 3:
        raise SystemExit(f"Expected exactly 3 ports, got {len(ports)}: {ports}")
    if len(set(ports)) != len(ports):
        raise SystemExit(f"Ports must be unique: {ports}")
    return ports


def is_port_in_use(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(0.3)
        return s.connect_ex(("127.0.0.1", port)) == 0


def read_pid_if_running(pid_file: Path) -> int | None:
    if not pid_file.exists():
        return None
    text = pid_file.read_text(encoding="utf-8").strip()
    if not text:
        return None
    try:
        pid = int(text)
    except ValueError:
        return None

    try:
        os.kill(pid, 0)
        return pid
    except ProcessLookupError:
        return None
    except PermissionError:
        return pid


def resolve_jar(repo_root: Path) -> Path:
    pattern = str(repo_root / "demo-consumer" / "target" / "demo-consumer-*.jar")
    jars = [Path(p) for p in glob.glob(pattern) if not p.endswith(".original")]
    if not jars:
        raise SystemExit("No runnable jar found. Use --build or run maven package first.")
    jars.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return jars[0]


def run_build(repo_root: Path, env: dict[str, str]) -> None:
    cmd = ["mvn", "-DskipTests", "-pl", "demo-consumer", "-am", "package"]
    print("Running build:", " ".join(cmd))
    proc = subprocess.run(cmd, cwd=repo_root, env=env)
    if proc.returncode != 0:
        raise SystemExit(f"Build failed with exit code {proc.returncode}")


def start_instance(
    repo_root: Path,
    jar: Path,
    port: int,
    env: dict[str, str],
    logs_dir: Path,
) -> int:
    log_file = logs_dir / f"demo-{port}.log"
    pid_file = logs_dir / f"demo-{port}.pid"

    existing_pid = read_pid_if_running(pid_file)
    if existing_pid is not None:
        raise SystemExit(f"Port {port} already has running instance in PID file: {existing_pid}")

    if is_port_in_use(port):
        raise SystemExit(f"Port {port} is already in use")

    cmd = [
        "java",
        "-jar",
        str(jar),
        f"--server.port={port}",
        f"--scheduler.instance-id=local-{port}",
    ]

    with log_file.open("ab") as logfp:
        proc = subprocess.Popen(
            cmd,
            cwd=repo_root,
            env=env,
            stdout=logfp,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )

    pid_file.write_text(str(proc.pid), encoding="utf-8")
    return proc.pid


def main() -> None:
    args = parse_args()
    ports = parse_ports(args.ports)

    if args.build and args.skip_build:
        raise SystemExit("Use either --build or --skip-build, not both")

    repo_root = Path(__file__).resolve().parents[1]
    logs_dir = repo_root / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)

    env = os.environ.copy()
    if args.java_home:
        env["JAVA_HOME"] = args.java_home
        env["PATH"] = str(Path(args.java_home) / "bin") + os.pathsep + env.get("PATH", "")

    env["DEMO_DB_URL"] = args.db_url
    env["DEMO_DB_USERNAME"] = args.db_username
    env["DEMO_DB_PASSWORD"] = args.db_password

    should_build = args.build or (not args.skip_build)
    if should_build:
        run_build(repo_root, env)

    jar = resolve_jar(repo_root)

    started = []
    try:
        for port in ports:
            pid = start_instance(repo_root, jar, port, env, logs_dir)
            started.append((port, pid))
            print(f"Started instance: port={port}, pid={pid}, log={logs_dir / f'demo-{port}.log'}")

        time.sleep(1.5)
        print("All instances launched.")
    except Exception as exc:
        for port, pid in started:
            try:
                os.killpg(pid, signal.SIGTERM)
            except Exception:
                try:
                    os.kill(pid, signal.SIGTERM)
                except Exception:
                    pass
            pid_file = logs_dir / f"demo-{port}.pid"
            if pid_file.exists():
                pid_file.unlink(missing_ok=True)
        raise SystemExit(f"Failed to start all instances: {exc}")


if __name__ == "__main__":
    main()
