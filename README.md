# user-task-scheduler

轻量级 Spring Boot Starter 分布式任务调度组件。

## 技术亮点

项目以 Spring Boot Starter 形态提供轻量级分布式任务调度能力，采用 MySQL 作为任务状态最终事实源、Redis 作为队列/租约/并发协作层，支持优先级调度、DAG 依赖编排、group 与 user 双维度限流和动态并发策略。

核心优势是接入成本低、无需独立调度平台，同时具备多实例高可用能力：通过 DB CAS、Redis lease、心跳恢复、队列 refill、失败重试和运行计数对账，提升任务执行的可靠性、可恢复性和扩展性。

## 项目功能

- 多任务组（group）隔离调度
- group 最大并发控制
- user 级并发控制（支持动态并发策略）
- 优先级调度（高优先级优先执行）
- 任务依赖编排（支持 DAG）
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

### 任务依赖

提交任务时可设置 `dependencies`。仅当所有依赖满足后，任务才可执行。

`targetState` 支持：

- `SUCCESS`
- `FAILED`
- `TERMINAL`

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
