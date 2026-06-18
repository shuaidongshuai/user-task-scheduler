# user-task-scheduler

轻量级 Spring Boot Starter 分布式任务调度组件。

## 技术亮点

项目以 Spring Boot Starter 形态提供轻量级分布式任务调度能力，采用 MySQL 作为任务状态最终事实源、Redis 作为队列/租约/并发协作层，支持优先级调度、DAG 依赖编排、group 与 user 双维度限流和动态并发策略。

核心优势是接入成本低、无需独立调度平台，同时具备多实例高可用能力：通过 DB CAS、Redis lease、心跳恢复、队列 refill、失败重试和运行计数对账，提升任务执行的可靠性、可恢复性和扩展性。

## 项目功能

- 多任务组（group）隔离调度
- group 最大并发控制
- user 级并发控制（支持动态并发策略）
- 多服务路由隔离消费（共享并发，分开 `time/ready` 队列）
- 同步执行任务提交（调用线程内执行，复用 group/user 并发限制）
- 优先级调度（高优先级优先执行）
- 任务依赖编排（支持 DAG）
- 任务超时自动取消（支持链式任务超时自动降级）
- 失败重试与延迟重试
- 宕机恢复（心跳超时自动回收）
- 可选业务状态短路（业务已完成/失败不重复执行）
- `ext_info` 跨重试透传（支持多阶段任务）

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
