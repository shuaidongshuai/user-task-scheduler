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

`SchedulerClient#executeSync(TaskSubmitRequest)` 会直接在当前调用线程执行 `TaskHandler`，不会进入异步 ready queue/worker 调度链路，但仍然共享同一个 group/user 并发限制。

适用场景：

- 调用方需要阻塞等待执行完成
- 希望复用现有 `TaskHandler`、执行记录、超时控制、并发控制

当前限制：

- 仅支持立即执行任务：`executeAt` 需要小于等于当前时间
- 不支持 `dependencies`
- 同步路径按单次执行处理，不走异步重试链路
- 若 group 或 user 并发已满，会直接抛出限流异常

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
