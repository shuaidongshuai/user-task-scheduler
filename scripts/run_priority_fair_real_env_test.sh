#!/usr/bin/env bash
set -euo pipefail

ENV_FILE_DEFAULT="/Users/chenmingdong01/Documents/密钥/test.env"
ENV_FILE="${ENV_FILE:-$ENV_FILE_DEFAULT}"
MAVEN_BIN="${MAVEN_BIN:-mvn}"
JAVA_HOME_VALUE="${SCRIPT_JAVA_HOME:-}"

USER_SPECS="${USER_SPECS:-heavy-user=0x8,10x8,20x8;user-b=0x4,10x4,20x4;user-c=0x4,10x4,20x4}"
HEAVY_USER="${HEAVY_USER:-heavy-user}"
USER_CONCURRENCY="${USER_CONCURRENCY:-2}"
GROUP_CONCURRENCY="${GROUP_CONCURRENCY:-6}"
DISPATCH_BATCH_SIZE="${DISPATCH_BATCH_SIZE:-100}"
TASK_SLEEP_MS="${TASK_SLEEP_MS:-5000}"
POLL_INTERVAL_MS="${POLL_INTERVAL_MS:-200}"
WARMUP_TIMEOUT_MS="${WARMUP_TIMEOUT_MS:-8000}"
OTHER_USER_START_DEADLINE_MS="${OTHER_USER_START_DEADLINE_MS:-8000}"
TEST_TIMEOUT_MS="${TEST_TIMEOUT_MS:-90000}"

usage() {
  cat <<EOF
Usage:
  scripts/run_priority_fair_real_env_test.sh [options]

Options:
  --env-file <path>                    env 文件路径，默认: ${ENV_FILE_DEFAULT}
  --user-specs <spec>                  用户任务分布
  --heavy-user <userId>                重用户 userId
  --user-concurrency <n>               用户并发
  --group-concurrency <n>              group 并发
  --dispatch-batch-size <n>            单轮调度批次
  --task-sleep-ms <ms>                 TaskHandler sleep 时长
  --poll-interval-ms <ms>              DB 轮询间隔
  --warmup-timeout-ms <ms>             重用户预热超时
  --other-user-start-deadline-ms <ms>  其他用户首次启动时限
  --test-timeout-ms <ms>               整体测试超时
  -h, --help

user-specs 格式:
  'heavy-user=0x20,10x20,20x20;user-b=0x6,10x6,20x6;user-c=0x6,10x6,20x6'

说明:
  - priority 范围 0~99，数字越小优先级越高
  - 每段是 priority x count
  - 第一个用户通常配置为重用户，或者通过 --heavy-user 指定

依赖环境变量:
  env 文件里需要提供:
    MYSQL_URL / MYSQL_USERNAME / MYSQL_PASSWORD
    REDIS_HOST / REDIS_PORT
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --env-file)
      ENV_FILE="$2"
      shift 2
      ;;
    --user-specs)
      USER_SPECS="$2"
      shift 2
      ;;
    --heavy-user)
      HEAVY_USER="$2"
      shift 2
      ;;
    --user-concurrency)
      USER_CONCURRENCY="$2"
      shift 2
      ;;
    --group-concurrency)
      GROUP_CONCURRENCY="$2"
      shift 2
      ;;
    --dispatch-batch-size)
      DISPATCH_BATCH_SIZE="$2"
      shift 2
      ;;
    --task-sleep-ms)
      TASK_SLEEP_MS="$2"
      shift 2
      ;;
    --poll-interval-ms)
      POLL_INTERVAL_MS="$2"
      shift 2
      ;;
    --warmup-timeout-ms)
      WARMUP_TIMEOUT_MS="$2"
      shift 2
      ;;
    --other-user-start-deadline-ms)
      OTHER_USER_START_DEADLINE_MS="$2"
      shift 2
      ;;
    --test-timeout-ms)
      TEST_TIMEOUT_MS="$2"
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

if [[ ! -f "$ENV_FILE" ]]; then
  echo "Env file not found: $ENV_FILE" >&2
  exit 1
fi

load_env_var() {
  local key="$1"
  local line
  line="$(grep -E "^${key}=" "$ENV_FILE" | tail -n 1 || true)"
  if [[ -n "$line" ]]; then
    printf -v "$key" '%s' "${line#*=}"
    export "$key"
  fi
}

load_env_var MYSQL_URL
load_env_var MYSQL_USERNAME
load_env_var MYSQL_PASSWORD
load_env_var REDIS_HOST
load_env_var REDIS_PORT

require_env() {
  local name="$1"
  if [[ -z "${!name:-}" ]]; then
    echo "Missing required env var: $name" >&2
    exit 1
  fi
}

require_env MYSQL_URL
require_env MYSQL_USERNAME
require_env MYSQL_PASSWORD
require_env REDIS_HOST
require_env REDIS_PORT

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

if [[ -n "$JAVA_HOME_VALUE" ]]; then
  export JAVA_HOME="$JAVA_HOME_VALUE"
  export PATH="$JAVA_HOME/bin:$PATH"
elif command -v /usr/libexec/java_home >/dev/null 2>&1; then
  JAVA_HOME_VALUE="$("/usr/libexec/java_home" -v 21)"
  export JAVA_HOME="$JAVA_HOME_VALUE"
  export PATH="$JAVA_HOME/bin:$PATH"
elif [[ -n "${JAVA_HOME:-}" ]]; then
  export PATH="$JAVA_HOME/bin:$PATH"
fi

echo "Running real-env priority fairness test"
echo "  env-file: $ENV_FILE"
echo "  heavy-user: $HEAVY_USER"
echo "  user-specs: $USER_SPECS"
echo "  user-concurrency: $USER_CONCURRENCY"
echo "  group-concurrency: $GROUP_CONCURRENCY"
echo "  task-sleep-ms: $TASK_SLEEP_MS"

DEMO_DB_URL="$MYSQL_URL" \
DEMO_DB_USERNAME="$MYSQL_USERNAME" \
DEMO_DB_PASSWORD="$MYSQL_PASSWORD" \
SPRING_DATA_REDIS_HOST="$REDIS_HOST" \
SPRING_DATA_REDIS_PORT="$REDIS_PORT" \
"$MAVEN_BIN" \
  -pl demo-consumer -am \
  -Dtest=PriorityFairSchedulingRealEnvTest \
  -Dsurefire.failIfNoSpecifiedTests=false \
  -Dfair.test.userSpecs="$USER_SPECS" \
  -Dfair.test.heavyUser="$HEAVY_USER" \
  -Dfair.test.userConcurrency="$USER_CONCURRENCY" \
  -Dfair.test.groupConcurrency="$GROUP_CONCURRENCY" \
  -Dfair.test.dispatchBatchSize="$DISPATCH_BATCH_SIZE" \
  -Dfair.test.taskSleepMs="$TASK_SLEEP_MS" \
  -Dfair.test.pollIntervalMs="$POLL_INTERVAL_MS" \
  -Dfair.test.warmupTimeoutMs="$WARMUP_TIMEOUT_MS" \
  -Dfair.test.otherUserStartDeadlineMs="$OTHER_USER_START_DEADLINE_MS" \
  -Dfair.test.testTimeoutMs="$TEST_TIMEOUT_MS" \
  test \
  -f "$REPO_ROOT/pom.xml"
