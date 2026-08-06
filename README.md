# user-task-scheduler

轻量级 Spring Boot Starter 分布式任务调度组件。

## 技术亮点

项目以 Spring Boot Starter 形态提供轻量级分布式任务调度能力，采用 MySQL 作为任务状态最终事实源、Redis 作为队列/租约/并发协作层，支持优先级调度、DAG 依赖编排、group 与 user 双维度限流和动态并发策略。

核心优势是接入成本低、无需独立调度平台，同时具备多实例高可用能力：通过 DB CAS、Redis lease、心跳恢复、队列 refill、失败重试和运行计数对账，提升任务执行的可靠性、可恢复性和扩展性。

## 项目功能

- 调度隔离：任务组（group）隔离调度
- 消费隔离：隔离消费且共享并发（适用于不同服务类型但共享同一个数据库和redis）
- 并发控制：组内最大并发控制，用户级并发控制（支持根据系统负载动态调整并发量）
- 任务调度：高优先级优先执行
- 任务编排：支持 DAG 任务组批量运行
- 任务降级：支持超时自动降级任务（自定义超时的降级任务）
- 任务重试：失败重试与延迟重试
- 长轮询任务：支持 `WAIT_HOLD` 模式，等待外部系统结果时释放 worker 线程但持续占用并发
- 故障恢复：宕机恢复（服务重启或redis数据丢失自动恢复）
- 扩展：`ext_info` 跨重试透传（支持多阶段任务）

## 核心优势

- 轻量接入：以 Starter 方式集成，无需额外调度平台
- 高可用：DB 持久化 + Redis 协作，支持实例故障恢复
- 可扩展：按 group、user 粒度配置并发与策略
- 易编排：内置任务依赖模型，支持复杂流程拆分
- 易落地：提供 demo-consumer 示例工程

## 快速开始

### 1. 引入依赖

```xml
<dependency>
  <groupId>org.dong</groupId>
  <artifactId>scheduler-starter</artifactId>
  <version>1.0-SNAPSHOT</version>
</dependency>
```

### 2. 初始化数据库

执行建表脚本：

- [schema-mysql.sql](/Users/chenmingdong01/Documents/github/user-task-scheduler/scheduler-starter/src/main/resources/sql/schema-mysql.sql)

### 3. 配置应用

```yaml
utask:
  scheduler:
    enabled: true
    dispatch-enabled: true
    # 不配置时保持历史行为；只有显式配置后才启用路由隔离
    dispatch-route: render-service
    auto-init-default-group: true
    default-group-code: public-group
    default-group-max-concurrency: 100
    default-group-user-base-concurrency: 4
    default-group-dispatch-batch-size: 100
    default-group-heartbeat-timeout-sec: 90
    default-group-lock-expire-sec: 120
    dispatch-interval-ms: 500
    # 是否开启非运行态 group fallback 扫描；默认 true，false 时仅停用 fallback，不影响正常调度。
    fallback-enabled: true
    # fallback 扫描周期（毫秒）。
    fallback-scan-interval-ms: 1000
    # 单次扫描最多处理的到期 fallback 任务数，避免扫描任务占用过久。
    fallback-scan-limit: 200
    # Handler 返回 keepCurrent/routeTo 的下一次检查时间，距当前时间的最小间隔（毫秒）。
    fallback-min-next-check-delay-ms: 1000
    # 单次 onGroupWaitTimeout 回调的最长执行时间（毫秒）；超时会终止该任务。
    fallback-policy-timeout-ms: 3000
    # 执行 onGroupWaitTimeout 回调的独立线程数，避免阻塞调度线程。
    fallback-callback-threads: 4
    # 回调线程池队列容量；当前必须为 0，使用直接交接并在饱和时延后任务检查。
    fallback-callback-queue-capacity: 0
    # 回调线程池拒绝任务时，fallback_check_at 延后的时间（毫秒）。
    fallback-executor-reject-backoff-ms: 5000
    recovery-interval-ms: 30000
    queue-refill-interval-ms: 15000
    worker-threads: 16
    heartbeat-interval-sec: 10
    default-retry-delay-sec: 15
    wait-hold-max-rounds: 1000
    wait-hold-default-delay-sec: 3
    default-execute-timeout-sec: 600
```

### 4. 实现任务执行器

```java
@Component
public class ImageRenderHandler implements TaskHandler {
    @Override
    public List<String> bizTypes() {
        return List.of("image.render");
    }

    @Override
    public TaskExecuteResult execute(SchedulerTask task) {
        // 执行业务逻辑
        return TaskExecuteResult.success();
    }

    @Override
    public GroupFallbackDecision onGroupWaitTimeout(SchedulerTask task) {
        return GroupFallbackDecision.routeTo("image-render-backup", null);
    }
}
```

### 5. 提交任务

```java
@Autowired
private SchedulerClient schedulerClient;

public void submitDemo() {
    TaskSubmitRequest req = new TaskSubmitRequest()
            .setGroupCode("image-render")
            .setUserId("user-1001")
            .setBizType("image.render")
            .setBizKey("biz-key-001")
            .setPriority(90)
            .setHoldMaxRounds(300)
            .setHoldRetryDelaySec(5)
            .setRetryDelaySec(20)
            .setFallbackCheckAt(LocalDateTime.now().plusSeconds(30))
            .setExtInfo("{\"prompt\":\"hello\"}");
    long taskId = schedulerClient.submit(req);
}
```

同步执行场景可直接在调用线程内执行任务：

```java
public void executeSyncDemo() {
    TaskSubmitRequest req = new TaskSubmitRequest()
            .setGroupCode("image-render")
            .setUserId("user-1001")
            .setBizType("image.render")
            .setBizKey("biz-key-sync-001")
            .setPriority(90)
            .setExecuteAt(LocalDateTime.now())
            .setExecuteTimeoutSec(60);
    long taskId = schedulerClient.executeSync(req);
}
```

## 使用说明

### group 配置

`scheduler_group_config` 是人工维护配置表，用于控制每个 group 的并发、心跳与调度参数。

关键字段：

- `group_code`：任务组编码
- `enabled`：是否启用该 group
- `max_concurrency`：group 最大并发
- `user_base_concurrency`：单用户基础并发
- `dynamic_user_limit_enabled`：是否开启动态 user 并发
- `load_strategy_json`：动态并发策略

### dispatchRoute 路由隔离

适用场景：

- 多个业务服务共享同一个 `scheduler_task` 表
- 需要共享同一个 `groupCode` 的全局并发、用户并发
- 但 A/B 服务只希望消费各自归属的任务

实现方式：

- `scheduler_task` 新增可空字段 `dispatch_route`
- 配置了 `dispatchRoute` 的服务，Redis `time/ready` 队列按 `groupCode + dispatchRoute` 分开
- 未配置 `dispatchRoute` 的服务，继续沿用旧版 `groupCode` 队列键
- `RUNNING` 并发计数、任务 lease 仍然按原来的 `groupCode` / `groupCode + userId` 共享

配置方式：

```yaml
utask:
  scheduler:
    dispatch-route: a-service
```

说明：

- 老服务不配 `dispatch-route` 时保持原行为，不会写入 `dispatch_route`，也不会切到新队列
- 新服务可配置自己的 route，例如 `a-service`、`b-service`
- 任务提交时未显式设置 `dispatchRoute`，只有当前服务配置了 `utask.scheduler.dispatch-route` 才会自动回退到该值
- 补偿回填任务时，仅扫描并回填当前 route 的任务，不会把别的 route 任务补进本机队列

数据库变更：

```sql
alter table scheduler_task
    add column dispatch_route varchar(64) null comment '调度路由（决定由哪类服务消费，为空表示兼容旧队列）' after group_code;

create index idx_group_route_status_time on scheduler_task(group_code, dispatch_route, status, execute_at);
create index idx_route_status_time on scheduler_task(dispatch_route, status, execute_at);
```

### 任务依赖

提交任务时可设置 `dependencies`。仅当所有依赖满足后，任务才可执行。

`targetState` 支持：

- `SUCCESS`
- `FAILED`
- `TERMINAL`

### 同步执行接口

`SchedulerClient#executeSync(TaskSubmitRequest)` 会直接在当前调用线程执行 `TaskHandler` 首轮，不会进入异步 ready queue/worker 线程池执行链路，但仍然共享同一个 group/user 并发限制。

适用场景：

- 调用方需要阻塞等待执行完成
- 希望复用现有 `TaskHandler`、执行记录、超时控制、并发控制

当前限制：

- 仅支持立即执行任务：`executeAt` 需要小于等于当前时间
- 不支持 `dependencies`
- 不支持 `fallbackCheckAt`；传入时抛出 `SYNC_FALLBACK_CHECK_UNSUPPORTED`
- 同步路径按单次执行处理，不走异步重试链路
- 若 group 或 user 并发已满，会直接抛出 `SchedulerException`
- 限流错误枚举为 `SchedulerErrorCode.CONCURRENCY_LIMIT`
- 限流错误码为 `SchedulerErrorCode.CONCURRENCY_LIMIT.getCode()`，当前值为 `429`
- 限流错误信息为 `SchedulerErrorCode.CONCURRENCY_LIMIT.getMessage()`

返回语义：

- 若首轮执行后任务状态为 `SUCCESS`，`executeSync(...)` 正常返回
- 若首轮执行返回 `WAIT_HOLD`，也视为本次同步提交成功，`executeSync(...)` 正常返回
- 但 `WAIT_HOLD` 场景下，返回仅表示“首轮已执行并成功进入后续轮询流程”，不表示任务整体生命周期已经结束
- 后续轮询仍由框架按 `WAIT_HOLD` 语义继续调度，并持续占用同一路 group/user 并发
- 若首轮执行后任务状态为 `FAILED`、`WAIT_RETRY` 等非成功态，则 `executeSync(...)` 抛异常

### 超时自动取消与链式降级

提交任务时可设置 `maxWaitSec`，表示任务从 `executeAt` 开始，最多还能等待多久；超过该时间后，如果任务仍处于等待态，会被系统自动标记为失败。

这个能力可以和链式任务一起使用：主任务超时自动失败后，后续依赖 `FAILED` 的降级任务会被自动拉起执行。

典型场景：

- 任务 A：主流程任务，设置 `maxWaitSec`
- 任务 B：兜底/降级任务，依赖任务 A 的 `FAILED`

这样当任务 A 长时间未执行成功、最终被系统超时取消后，任务 B 会自动接管。

批量链式提交时可直接这样编排：

```java
BatchSubmitRequest request = new BatchSubmitRequest(List.of(
        new BatchSubmitTaskRequest()
                .setClientTaskRef("main")
                .setGroupCode("image-render")
                .setUserId("user-1001")
                .setBizType("image.render")
                .setBizKey("biz-main")
                .setExecuteAt(LocalDateTime.now())
                .setMaxWaitSec(300),
        new BatchSubmitTaskRequest()
                .setClientTaskRef("fallback")
                .setGroupCode("image-render")
                .setUserId("user-1001")
                .setBizType("image.render.fallback")
                .setBizKey("biz-fallback")
                .setExecuteAt(LocalDateTime.now())
                .setDependencies(List.of(
                        new BatchSubmitDependencyRequest(null, "main", DependencyTargetState.FAILED)
                ))
));

schedulerClient.submitBatch(request);
```

### 非运行态动态 Group 降级

异步任务可通过 `fallbackCheckAt` 指定首次策略检查的绝对时间。检查到期且任务仍处于
`PENDING`、`RUNNABLE` 或 `WAIT_RETRY` 时，框架调用 `TaskHandler#onGroupWaitTimeout`。

Handler 可返回：

- `routeTo(groupCode, nextCheckAt)`：切换到另一个已启用 Group，可选继续检查。
- `keepCurrent(nextCheckAt)`：保留当前 Group，并在未来再次检查。
- `stopChecking()`：停止后续降级检查。
- `fail(code, message)`：将任务置为失败并传播依赖终态。

`RUNNING` 与仍持有并发的 `WAIT_HOLD` 不会触发该回调。回调应快速、无副作用并响应线程中断；
多实例竞争时回调可能重复计算，但只有符合任务快照的数据库 CAS 能生效。
若回调抛出异常，框架不会将业务任务置为失败，而是保留当前 group，并在
`10 × fallback-min-next-check-delay-ms` 后重试 fallback 检查；返回 `null` 或非法决策仍视为接入错误并使任务失败。

### 执行期重试切换 Group

当 `TaskHandler#execute` 发现当前 group 不适合继续处理时，可返回
`TaskExecuteResult.retryableOnGroup(errorCode, errorMessage, targetGroupCode)`。框架验证 target group
已启用后，原子地将任务改为 `WAIT_RETRY` 和目标 `group_code`，下一次执行由目标 group 调度：

```java
return TaskExecuteResult.retryableOnGroup(
        "UPSTREAM_OVERLOADED", "retry through backup group", "image-render-backup");
```

若无需记录错误原因，也可简写为 `TaskExecuteResult.retryableOnGroup("image-render-backup")`。

这仅适用于可重试执行：本次执行的并发始终从 source group 释放，避免把 `WAIT_HOLD` 的持有并发错误地
迁移到 target group。目标 group 不存在、未启用、与 source 相同或重试 CAS 失败时，任务会失败并记录
`EXECUTION_TARGET_GROUP_INVALID`。

已有数据库升级时，先执行：

- [upgrade-group-fallback.sql](/Users/chenmingdong01/Documents/github/user-task-scheduler/scheduler-starter/src/main/resources/sql/upgrade-group-fallback.sql)

#### 本地 HTTP 冒烟脚本

先通过 Reactor 构建当前 Starter 与 Demo，再启动生成的 Boot Jar；不要单独执行
`mvn -pl demo-consumer spring-boot:run`，否则可能加载本地 Maven 仓库中旧的
`scheduler-starter` SNAPSHOT。启动时确保 `dispatch-enabled=true`、`fallback-enabled=true`；若服务
配置了 `dispatch-route`，脚本参数必须使用相同值。脚本需要可连接的 MySQL（用于状态断言）以及
`requests`、`pymysql` Python 依赖。

```bash
mvn -pl demo-consumer -am -DskipTests package
java -jar demo-consumer/target/demo-consumer-1.0-SNAPSHOT.jar \
  --utask.scheduler.dispatch-enabled=true \
  --utask.scheduler.fallback-enabled=true
```

```bash
python3 scripts/run_group_fallback_http_smoke.py \
  --base-url http://127.0.0.1:8099
```

默认不使用 `dispatch-route`。只有服务显式配置了 route 隔离时，才在启动参数和脚本中同时传入同一个值：

```bash
--utask.scheduler.dispatch-route=render-service
```

脚本会创建唯一的 source/target Group，通过 HTTP 验证普通任务成功、未来执行的任务在
`fallbackCheckAt` 到期后切换到 target Group 并写入审计日志，以及执行期返回重试切换 group 后最终成功。
默认清理本次任务、业务数据与测试 Group；传入 `--keep-data` 可保留现场排查。

### WAIT_HOLD：长轮询/外部异步任务

适用场景：

- 业务先提交一个外部异步任务
- 后续需要每隔几秒轮询外部结果
- 整个生命周期都希望持续占用同一路 group/user 并发
- 但不希望轮询等待期间长期占住 worker 线程

处理方式：

- `TaskHandler` 返回 `TaskExecuteResult.waitHold()`
- 框架将任务状态写为 `WAIT_HOLD`
- 同时保留 group/user running 计数，不释放并发
- 释放当前这轮执行 lease 和 worker 线程
- 按任务上的 `hold_retry_delay_sec` 重新计算下一次 `execute_at`
- 任务重新进入 time queue，到期后再被 promote 到 ready queue
- 调度恢复时只抢本轮 task lease，不重复增加并发计数

任务级参数：

- `holdMaxRounds`：最多允许多少轮 `WAIT_HOLD`
- `holdRetryDelaySec`：每轮等待多少秒后再继续执行

这两个参数不传时，分别回退到全局配置：

- `utask.scheduler.wait-hold-max-rounds`
- `utask.scheduler.wait-hold-default-delay-sec`

重要语义：

- `WAIT_HOLD` 与 `WAIT_RETRY` 不同
- `WAIT_RETRY` 是失败后重试，会释放并发
- `WAIT_HOLD` 是运行中挂起，不释放并发
- `ext_info` 仍然完全由业务侧维护，框架只负责透传和持久化

推荐业务写法：

- 首次执行：提交远端任务，把 `remoteJobId` 写入 `ext_info`，返回 `waitHold()`
- 后续执行：根据 `ext_info` 直接轮询远端任务，不重复 submit
- 终态时返回 `success()` 或 `failed(...)`

### 业务状态短路（可选）

可实现 `BusinessTaskStateProvider`，在执行前检查业务状态：

- `SUCCESS`：任务直接成功
- `FAILED`：任务直接失败
- `NEED_RUNNING`：进入正常调度执行

### Demo

完整接入示例见：

- [demo-consumer/README.md](/Users/chenmingdong01/Documents/github/user-task-scheduler/demo-consumer/README.md)

## 技术文档

详细技术方案见：

- [docs/技术方案设计.md](/Users/chenmingdong01/Documents/github/user-task-scheduler/docs/技术方案设计.md)
