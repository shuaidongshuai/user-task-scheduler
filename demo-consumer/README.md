# demo-consumer

这个 demo 展示如何在业务项目中接入 `scheduler-starter`。

## 1. 前置条件

- MySQL 8+
- Redis 6+
- JDK 21（你可以使用）：
  - `/Users/chenmingdong01/Library/Java/JavaVirtualMachines/openjdk-21.0.1/Contents/Home`

## 2. 初始化数据库

1. 创建数据库：

```sql
CREATE DATABASE scheduler_demo DEFAULT CHARACTER SET utf8mb4;
```

2. 执行 starter 表结构：

- [schema-mysql.sql](/Users/chenmingdong01/Documents/github/user-task-scheduler/scheduler-starter/src/main/resources/sql/schema-mysql.sql)

3. 执行 demo 表与 group 初始化：

- [demo-schema.sql](/Users/chenmingdong01/Documents/github/user-task-scheduler/demo-consumer/src/main/resources/sql/demo-schema.sql)

## 3. 构建并启动

在仓库根目录先安装 starter：

```bash
export JAVA_HOME=/Users/chenmingdong01/Library/Java/JavaVirtualMachines/openjdk-21.0.1/Contents/Home
mvn -DskipTests -pl scheduler-starter -am install
```

启动 demo：

```bash
export JAVA_HOME=/Users/chenmingdong01/Library/Java/JavaVirtualMachines/openjdk-21.0.1/Contents/Home
export DEMO_DB_URL='jdbc:mysql://127.0.0.1:3306/scheduler_demo?useUnicode=true&characterEncoding=utf8&serverTimezone=SYSTEM&allowPublicKeyRetrieval=true&useSSL=false'
export DEMO_DB_USERNAME='root'
export DEMO_DB_PASSWORD='root'
# Production baseline Hikari tuning (override by your DB capacity):
export DEMO_DB_POOL_NAME='scheduler-hikari'
export DEMO_DB_MIN_IDLE='12'
export DEMO_DB_MAX_POOL_SIZE='40'
export DEMO_DB_MAX_LIFETIME_MS='1700000'
export DEMO_DB_IDLE_TIMEOUT_MS='300000'
export DEMO_DB_KEEPALIVE_MS='300000'
export DEMO_DB_VALIDATION_TIMEOUT_MS='5000'
export DEMO_DB_CONNECTION_TIMEOUT_MS='10000'
export DEMO_DB_INIT_FAIL_TIMEOUT_MS='1'
export DEMO_DB_LEAK_DETECT_MS='0'
mvn -pl demo-consumer spring-boot:run
```

## 4. 测试接口

1. 提交任务：

```bash
curl -X POST 'http://127.0.0.1:8088/demo/submit' \
  -H 'Content-Type: application/json' \
  -d '{
    "groupCode":"demo-group",
    "userId":"u1",
    "bizKey":"order-1001",
    "priority":90,
    "retryDelaySec":20,
    "extInfo":"{\"step\":0}",
    "payload":"hello"
  }'
```

说明：

- `retryDelaySec` 为单任务重试间隔，单位秒
- 例如设置为 `20`，则任务失败后的下一次调度时间会延后 20 秒
- 若不传，则使用全局配置 `scheduler.default-retry-delay-sec`
- 幂等键为 `bizType + bizKey`（demo 中 `bizType` 固定为 `demo.biz.process`）
- `extInfo` 为可选字符串扩展信息，任务重试时可使用上轮执行写回的最新值

2. 查询业务状态：

```bash
curl 'http://127.0.0.1:8088/demo/biz/{bizKey}'
```

3. 手动改业务状态（验证 BusinessTaskStateProvider 短路能力）：

```bash
curl -X POST 'http://127.0.0.1:8088/demo/biz/{bizKey}/status/SUCCESS'
```

## 5. 说明

- `DemoTaskHandler`：示例业务执行逻辑
- `DemoBusinessTaskStateProvider`：示例业务状态查询（可选）
- `forceRetry=true` 时，会模拟可重试失败
