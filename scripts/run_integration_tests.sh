#!/usr/bin/env bash
set -euo pipefail

MYSQL_CONTAINER="uts-mysql-test"
REDIS_CONTAINER="uts-redis-test"
MYSQL_PORT="${MYSQL_PORT:-33306}"
REDIS_PORT="${REDIS_PORT:-36379}"
MYSQL_ROOT_PASSWORD="${MYSQL_ROOT_PASSWORD:-root}"
MYSQL_DATABASE="${MYSQL_DATABASE:-scheduler_demo}"
MYSQL_IMAGE="${MYSQL_IMAGE:-mysql:8.4}"
REDIS_IMAGE="${REDIS_IMAGE:-redis:7.2}"
KEEP_CONTAINERS="false"
MAVEN_BIN="${MAVEN_BIN:-/Applications/IntelliJ IDEA.app/Contents/plugins/maven/lib/maven3/bin/mvn}"
JAVA_HOME_DEFAULT="/Applications/IntelliJ IDEA.app/Contents/jbr/Contents/Home"
JAVA_HOME_VALUE="${SCRIPT_JAVA_HOME:-$JAVA_HOME_DEFAULT}"
TEST_PATTERN="${TEST_PATTERN:-DependencyIntegrationTest}"

usage() {
  cat <<EOF
Usage:
  scripts/run_integration_tests.sh [--keep-containers] [--test-pattern DependencyIntegrationTest]

Options:
  --keep-containers       Do not stop/remove MySQL and Redis containers after test
  --test-pattern <name>   Surefire test pattern (default: DependencyIntegrationTest)
  -h, --help

Environment overrides:
  MYSQL_PORT, REDIS_PORT, MYSQL_ROOT_PASSWORD, MYSQL_DATABASE
  MYSQL_IMAGE, REDIS_IMAGE, MAVEN_BIN, SCRIPT_JAVA_HOME
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --keep-containers)
      KEEP_CONTAINERS="true"
      shift
      ;;
    --test-pattern)
      TEST_PATTERN="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage
      exit 1
      ;;
  esac
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

require_cmd() {
  local cmd="$1"
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "Missing required command: $cmd" >&2
    exit 1
  fi
}

require_cmd docker

if [[ ! -x "$MAVEN_BIN" ]]; then
  echo "Maven not executable: $MAVEN_BIN" >&2
  exit 1
fi

export JAVA_HOME="$JAVA_HOME_VALUE"
export PATH="$JAVA_HOME/bin:$PATH"

cleanup() {
  if [[ "$KEEP_CONTAINERS" == "true" ]]; then
    return
  fi
  docker rm -f "$MYSQL_CONTAINER" >/dev/null 2>&1 || true
  docker rm -f "$REDIS_CONTAINER" >/dev/null 2>&1 || true
}

trap cleanup EXIT

ensure_container_absent() {
  local name="$1"
  docker rm -f "$name" >/dev/null 2>&1 || true
}

wait_for_mysql() {
  local retries=60
  for _ in $(seq 1 "$retries"); do
    if docker exec "$MYSQL_CONTAINER" mysqladmin ping -uroot "-p$MYSQL_ROOT_PASSWORD" --silent >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "MySQL did not become ready in time" >&2
  exit 1
}

wait_for_redis() {
  local retries=30
  for _ in $(seq 1 "$retries"); do
    if docker exec "$REDIS_CONTAINER" redis-cli ping >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "Redis did not become ready in time" >&2
  exit 1
}

echo "Starting MySQL container on port $MYSQL_PORT"
ensure_container_absent "$MYSQL_CONTAINER"
docker run -d \
  --name "$MYSQL_CONTAINER" \
  -e "MYSQL_ROOT_PASSWORD=$MYSQL_ROOT_PASSWORD" \
  -e "MYSQL_DATABASE=$MYSQL_DATABASE" \
  -p "$MYSQL_PORT:3306" \
  "$MYSQL_IMAGE" >/dev/null
wait_for_mysql

echo "Starting Redis container on port $REDIS_PORT"
ensure_container_absent "$REDIS_CONTAINER"
docker run -d \
  --name "$REDIS_CONTAINER" \
  -p "$REDIS_PORT:6379" \
  "$REDIS_IMAGE" >/dev/null
wait_for_redis

echo "Initializing database schema"
docker exec -i "$MYSQL_CONTAINER" mysql -uroot "-p$MYSQL_ROOT_PASSWORD" "$MYSQL_DATABASE" \
  -h127.0.0.1 \
  < "$REPO_ROOT/scheduler-starter/src/main/resources/sql/schema-mysql.sql"
docker exec -i "$MYSQL_CONTAINER" mysql -uroot "-p$MYSQL_ROOT_PASSWORD" "$MYSQL_DATABASE" \
  -h127.0.0.1 \
  < "$REPO_ROOT/demo-consumer/src/main/resources/sql/demo-schema.sql"

echo "Running scheduler-starter unit tests"
"$MAVEN_BIN" -pl scheduler-starter test -q -f "$REPO_ROOT/pom.xml"

echo "Running demo-consumer integration tests"
DEMO_DB_URL="jdbc:mysql://127.0.0.1:${MYSQL_PORT}/${MYSQL_DATABASE}?useUnicode=true&characterEncoding=utf8&serverTimezone=Asia/Shanghai&allowPublicKeyRetrieval=true&useSSL=false" \
DEMO_DB_USERNAME="root" \
DEMO_DB_PASSWORD="$MYSQL_ROOT_PASSWORD" \
SPRING_DATA_REDIS_HOST="127.0.0.1" \
SPRING_DATA_REDIS_PORT="$REDIS_PORT" \
"$MAVEN_BIN" -pl demo-consumer -am -Dtest="$TEST_PATTERN" -Dsurefire.failIfNoSpecifiedTests=false test -q -f "$REPO_ROOT/pom.xml"

echo "Integration tests passed."
