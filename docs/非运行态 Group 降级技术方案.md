# UTask 非运行态动态 Group 降级技术方案

## 2026-07-23 11:08

> 2026-07-23 16:18 修订：补充 fallback 终态依赖传播、Handler 超时与执行隔离、Redis 终态清理、stale queue 精确清理、扫描索引与输入边界。
>
> 2026-07-23 17:02 修订：executor 拒绝改为非终态退避，硬超时改为事务内锁定实际快照，明确固定并发槽位的回调执行模型。

## 1. 方案结论

首期只支持任务在**非运行、且不持有运行并发**时切换 Group：

- 提交任务时仍只传一个初始 `groupCode`，不传候选 Group 列表。
- 调用方可选传入绝对时间 `fallbackCheckAt`。
- 当任务处于 `PENDING/RUNNABLE/WAIT_RETRY` 且 `fallbackCheckAt` 到期时，框架调用一次 `TaskHandler.onGroupWaitTimeout`。
- Handler 返回具体目标 Group 和下一次检查时间；框架通过数据库 CAS 修改原任务的 `group_code`，清理旧 Group 队列引用，再加入新 Group 的调度队列。
- `RUNNING` 不调用降级接口；`TaskExecuteResult` 不新增 GROUP_FALLBACK 返回类型。
- `WAIT_HOLD` 虽然不是 RUNNING，但仍保留运行并发计数，因此不允许降级。
- `executeSync` 不支持 `fallbackCheckAt`，其余行为完全不变。

这版不改 Worker 执行结果状态机、不迁移运行 lease/并发、不处理运行中切组，改造范围小、并发边界清晰。未来出现真实的“业务执行后要求换 Group”需求时，再作为第二阶段单独设计。

## 2. 目标与非目标

### 2.1 目标

1. 支持排队任务在业务指定时间到期后动态切换 Group。
2. 目标 Group 完全由业务 Handler 决定，框架不维护降级列表。
3. 切组复用原 `taskId/taskNo`，不创建新任务。
4. 目标 Group 用户或组并发已满时，任务继续在目标 Group 排队。
5. 多实例竞争下，同一个检查时间最多一次策略结果生效。
6. 未使用该能力的旧任务行为完全兼容。

### 2.2 非目标

- 不支持 `RUNNING` 状态切组。
- 不扩展 `TaskExecuteResult`，Handler 执行结果不能要求切组。
- 不支持 `WAIT_HOLD` 切组。
- 不根据执行超时、心跳超时、普通异常自动切组。
- 不支持 `executeSync` 降级。
- 不新增候选 `groupCodes`、`groupWaitTimeoutSec` 或 Group 下标。
- 切换 Group 时不修改 `dispatchRoute`。

## 3. 时间与状态语义

### 3.1 时间字段

| 字段 | 语义 | 到期行为 |
|------|------|------|
| `executeAt` | 当前任务最早调度时间 | 到期后可进入 ready 队列 |
| `waitDeadlineAt` | `maxWaitSec` 计算得到的任务级硬截止线 | 整个任务直接失败，不调用降级策略 |
| `fallbackCheckAt` | 下一次调用业务降级策略的绝对时间 | 调用 `onGroupWaitTimeout` |

规则：

- `fallbackCheckAt == null`：任务不启用降级策略。
- `waitDeadlineAt <= now`：硬超时优先，直接失败为 `SCHEDULE_WAIT_TIMEOUT`。
- 策略调用后必须把 `fallbackCheckAt` 更新为未来时间或清空，禁止保留已过期值形成热循环。
- 若同时设置硬截止线，非空的下一次 `fallbackCheckAt` 必须早于 `waitDeadlineAt`。
- 首次 `fallbackCheckAt` 允许早于或等于提交时间，表示提交后尽快检查；也允许早于 `executeAt`，因为 `PENDING` 任务可在依赖等待期间切组。
- 后续决策返回的 `nextFallbackCheckAt` 必须不早于 `now + fallbackMinNextCheckDelayMs`。

### 3.2 状态范围

| 状态 | 是否扫描 | 是否允许切组 | Redis/并发处理 |
|------|----------|--------------|----------------|
| `PENDING` | 是 | 是 | 清理可能存在的旧队列引用；保持 PENDING，不强制进入 ready |
| `RUNNABLE` | 是 | 是 | 清理旧队列，按新 Group 和 executeAt 重新入 time/ready 队列 |
| `WAIT_RETRY` | 是 | 是 | 清理旧队列，按新 Group 和 executeAt 重新入 time/ready 队列 |
| `RUNNING` | 否 | 否 | 不调用策略，不触碰 lease/running 计数 |
| `WAIT_HOLD` | 否 | 否 | 仍持有 Group/User 并发，保持现有轮转逻辑 |
| 终态 | 否 | 否 | 不再迁移 |

“非运行态清理 Redis”仅指删除该任务在旧 Group 的 time/ready 队列成员以及必要的 active-user 队列索引，不得直接删除 task lease、Group running 或 User running 计数。

`WAIT_RETRY` 刚由 Worker 写入时，旧执行的 finally 可能还在释放 lease；降级扫描只迁移队列和数据库 Group，不主动处理该 lease。目标 Group Dispatcher 若短暂遇到旧 lease，会获取失败并在后续轮次重试。

降级扫描的 SQL 只包含 `PENDING/RUNNABLE/WAIT_RETRY`。任务一旦进入 `SUCCESS/FAILED/CANCELLED` 终态，或进入 `RUNNING/WAIT_HOLD`，即使数据库中仍有历史 `fallbackCheckAt`，也不会再被扫描或调用降级接口。所有 fallback 引起的终态更新同时清空 `fallbackCheckAt`。

## 4. API 设计

### 4.1 提交参数

`TaskSubmitRequest` 与 `BatchSubmitTaskRequest` 新增：

```java
private LocalDateTime fallbackCheckAt;
```

含义：

- `fallbackCheckAt`：首次检查绝对时间；为空不启用。

校验：

- 若 `waitDeadlineAt` 不为空，要求 `fallbackCheckAt < waitDeadlineAt`。
- `executeSync` 传入 `fallbackCheckAt` 时抛出 `SYNC_FALLBACK_CHECK_UNSUPPORTED`。
- 首次 `fallbackCheckAt <= now` 按“提交后尽快检查”处理，不拒绝提交。

不设置框架级最大切组或策略次数。Handler 每次决策都必须明确返回是否继续检查：非空 `fallbackCheckAt` 表示下一次调用时间，空表示以后不再调用降级策略。重复检查只可在等待态且 Handler 显式返回下一次时间时发生；任务进入成功、失败或取消终态后，状态条件会将其永久排除，不会继续轮询降级接口。

### 4.2 Handler 默认方法

```java
public interface TaskHandler {
    List<String> bizTypes();

    TaskExecuteResult execute(SchedulerTask task) throws Exception;

    default GroupFallbackDecision onGroupWaitTimeout(SchedulerTask task) {
        return GroupFallbackDecision.stopChecking();
    }
}
```

新增的 `fallbackCheckAt`、`fallbackPolicyCount`、`groupFallbackCount` 直接映射到 `SchedulerTask`；当前 Group、任务硬截止时间和业务信息也都可从该对象获取，因此不再引入 `GroupFallbackContext`。

`SchedulerTask` 在回调中仅是当次数据库扫描快照。Handler 不得修改对象并期望修改自动落库；目标 Group 和下一次检查时间只能通过 `GroupFallbackDecision` 返回。

```java
public enum GroupFallbackAction {
    ROUTE,
    KEEP_CURRENT,
    STOP_CHECKING,
    FAIL
}

public record GroupFallbackDecision(
        GroupFallbackAction action,
        String targetGroupCode,
        LocalDateTime nextFallbackCheckAt,
        String errorCode,
        String errorMessage
) {
    public static GroupFallbackDecision routeTo(String groupCode, LocalDateTime nextCheckAt) { /* ... */ }
    public static GroupFallbackDecision keepCurrent(LocalDateTime nextCheckAt) { /* ... */ }
    public static GroupFallbackDecision stopChecking() { /* ... */ }
    public static GroupFallbackDecision fail(String code, String message) { /* ... */ }
}
```

决策规则：

- `ROUTE`：目标 Group 必填、已启用且必须不同于当前 Group；可同时设置下一次检查时间。
- `KEEP_CURRENT`：不切组，只把检查时间更新为未来时间。
- `STOP_CHECKING`：不切组，清空检查时间。
- `FAIL`：通过统一终态事务将任务标记 FAILED，解锁/终止下游依赖，事务提交后清理当前队列引用。
- Handler 返回空、非法组合、过去时间或抛异常时，任务以策略错误失败，避免无限扫描。
- `targetGroupCode` 去除首尾空白后长度不得超过 64；`errorCode` 不得超过 64；`errorMessage` 超过 1024 字符时截断并记录指标，避免落库失败导致重复扫描。

Handler 回调属于“可能重复计算、CAS 单次生效”语义，必须快速、无副作用，不得直接修改 `SchedulerTask`、数据库或 Redis。框架在独立有界 executor 中执行回调并强制超时；Handler 已开始后超时走带快照条件的策略失败终态流程，executor 在 Handler 开始前拒绝则只退避下一次检查时间，不失败业务任务。

## 5. 数据库设计

### 5.1 scheduler_task 变更

```sql
alter table scheduler_task
    add column fallback_check_at datetime null
        comment '下次调用等待降级策略的绝对时间；为空表示不启用' after wait_deadline_at,
    add column fallback_policy_count int not null default 0
        comment 'onGroupWaitTimeout累计调用并成功落库的次数' after fallback_check_at,
    add column group_fallback_count int not null default 0
        comment '任务实际成功切换group的累计次数' after fallback_policy_count,
    add index idx_route_status_fallback(dispatch_route, status, fallback_check_at, id);
```

`group_code` 始终表示任务当前排队所属 Group。无需候选组 JSON、groupIndex 或 groupWaitTimeoutSec。

### 5.2 降级切组日志

```sql
create table scheduler_task_group_fallback_log (
    id bigint primary key auto_increment comment '降级切组日志主键ID',
    task_id bigint not null comment '原调度任务ID；切组前后复用同一个scheduler_task.id',
    task_no varchar(64) not null comment '原调度任务唯一号；切组不会创建新taskNo',
    source_group_code varchar(64) not null comment '切组前任务排队所属groupCode',
    target_group_code varchar(64) not null comment '切组后任务排队所属groupCode；必须与source_group_code不同',
    previous_fallback_check_at datetime not null comment '触发本次策略调用的原fallbackCheckAt',
    next_fallback_check_at datetime null comment '切组后下一次策略检查绝对时间；为空表示停止检查',
    task_status varchar(32) not null comment '切组发生时任务状态：PENDING/RUNNABLE/WAIT_RETRY',
    fallback_count int not null comment '本次成功后累计实际切组次数',
    create_time datetime not null default current_timestamp comment '数据库切组成功时间',
    index idx_task_time(task_id, create_time),
    index idx_source_target_time(source_group_code, target_group_code, create_time)
) engine=InnoDB default charset=utf8mb4
  comment='非运行任务Group实际切换审计日志；每次成功从一个Group切换到另一个Group记录一行，不记录KEEP_CURRENT、STOP_CHECKING、FAIL、策略异常或未通过CAS的决策';
```

任务主表 CAS 更新和日志插入必须位于同一数据库事务中。该表不需要 executeNo：降级发生时任务未在执行。上线前必须对 `dispatch_route is null` 和非空 route 两类扫描 SQL 执行 MySQL `EXPLAIN`，确认命中复合索引；若 `<=>` 在目标 MySQL 版本无法得到稳定计划，Repository 按 null/非 null 拆成两条 SQL。

## 6. 核心服务

新增 `GroupFallbackService`：

```java
FallbackApplyResult applyWaitingDecision(
        SchedulerTask snapshot,
        GroupFallbackDecision decision,
        LocalDateTime now);
```

职责：

1. 检查任务硬截止时间。
2. 校验状态、决策、目标 Group 和下一次检查时间。
3. `ROUTE/KEEP_CURRENT/STOP_CHECKING` 执行带快照条件的 CAS；`ROUTE` 在同一事务写切组日志。
4. `FAIL`、Handler 缺失、非法决策、回调异常/超时通过带快照条件的终态 CAS 失败任务，并在同一数据库事务内调用 `TaskDependencyService.onUpstreamTaskTerminal`。
5. executor 在 Handler 开始前拒绝时，执行带快照条件的非终态退避 CAS，仅推迟 `fallbackCheckAt`，不增加策略次数、不修改 Group/status。
6. 硬截止时间到期时复用增强后的 `TaskStateService.markFailedByWaitDeadline`：在事务内锁定实际终态迁移前快照，由方法内部保留该快照供提交后清理，同时完成依赖传播，不另写宽松失败流程。
7. 事务提交后使用实际发生迁移前的快照精确清理 Group 队列引用；终态分支也必须清理。
8. 非终态决策根据数据库新快照和状态写入新 Group 队列；终态决策入队依赖传播产生的下游任务。
9. 输出统一日志、指标和错误码。

终态 CAS 必须使用与 ROUTE 相同的 `id + waiting status + groupCode + fallbackCheckAt + version` 快照条件，同时清空 `fallback_check_at`。禁止直接复用可以更新 `RUNNING` 的通用 `markFailed`，避免 Handler 返回前 Dispatcher 已启动任务时误杀运行任务。

硬超时使用如下统一终态编排，fallback scan 和现有 wait-timeout job 只调用该方法：

```java
boolean markFailedByWaitDeadline(Long taskId, LocalDateTime now);
```

方法内部使用 `TransactionTemplate`：

1. `select ... from scheduler_task where id=? for update` 读取并锁定当前任务，得到 `terminalSourceSnapshot`。
2. 校验当前状态仍为 `PENDING/RUNNABLE/WAIT_RETRY` 且 `waitDeadlineAt <= now`。
3. 按 `id + status + version + waitDeadlineAt <= now` 更新为 FAILED，并清空 `fallback_check_at`。
4. 在同一事务内调用 `onUpstreamTaskTerminal(FAILED)`，返回下游入队任务。
5. 事务提交后用 `terminalSourceSnapshot` 调用 `removeQueueReferences`，然后入队下游任务。

若 ROUTE 先提交，硬超时事务锁定的是新 Group 快照；若硬超时先锁行，ROUTE CAS 在等待后必须因终态/version 变化而失败。因此 Redis 始终按实际发生终态迁移前的 Group 清理。

不修改 `WorkerService` 的执行结果处理，不新增 TaskExecutionStatus，也不修改 ConcurrencyGuard。

## 7. 扫描与状态迁移

### 7.1 定时扫描

```sql
select id
  from scheduler_task
 where dispatch_route <=> ?
   and status in ('PENDING','RUNNABLE','WAIT_RETRY')
   and fallback_check_at is not null
   and fallback_check_at <= ?
 order by fallback_check_at asc, id asc
 limit ?;
```

处理顺序：

1. 重新读取任务快照。
2. 若 `waitDeadlineAt <= now`，直接按任务硬超时失败，不调用 Handler。
3. 按 `bizType` 从当前 `dispatchRoute` 的 Handler Registry 获取 Handler。
4. 扫描控制器在独立有界 `fallbackCallbackExecutor` 中最多同时启动 `fallbackCallbackThreads` 个 `onGroupWaitTimeout`。
5. 校验决策并执行 CAS。
6. CAS 成功后更新队列；CAS 失败不执行任何 Redis 迁移。

fallback scan 使用专用 `fallbackTaskScheduler`，不与 dispatch、wait-timeout、recover、refill 共用调度线程。`fallbackCallbackExecutor` 使用固定大小线程池，core/max 均为 `fallbackCallbackThreads`，预启动所有核心线程，queue capacity 为 0（`SynchronousQueue`），不允许将整批任务预先堆积在 executor 队列中。

扫描控制器使用固定并发槽位模型：

1. 从本批 ID 中填充最多 `fallbackCallbackThreads` 个 in-flight 槽位，同一实例内用 taskId 去重。
2. 每个 Callable 在真正进入 Handler 前写入 `startedAt`，超时从 `startedAt` 计算，不从查询或提交 executor 时计算。
3. 控制器收集已完成结果；完成一个即释放槽位并从本批补充一个，直到本批处理完毕。
4. `now - startedAt >= fallbackPolicyTimeoutMs` 时调用 `Future.cancel(true)` 并走回调超时终态 CAS；Handler 必须响应中断。
5. 如果 Handler 忽略中断，对应 executor 线程可能继续被占用；后续新任务提交被拒绝时仅做非终态退避，不占用 Worker executor，不失败业务任务。

在 Handler 正常响应中断的最坏情况下，一批最长回调耗时约为 `ceil(fallbackScanLimit / fallbackCallbackThreads) * fallbackPolicyTimeoutMs`；默认配置下约 150 秒，不再是串行处理的 600 秒。跨实例重复计算仍依赖快照 CAS 保证单次生效。

### 7.2 ROUTE 原子迁移

```sql
update scheduler_task
   set group_code=?,
       fallback_check_at=?,
       fallback_policy_count=fallback_policy_count+1,
       group_fallback_count=group_fallback_count+1,
       update_time=now(),
       version=version+1
 where id=?
   and status in ('PENDING','RUNNABLE','WAIT_RETRY')
   and group_code=?
   and fallback_check_at=?
   and version=?
   and (wait_deadline_at is null or wait_deadline_at > now());
```

`KEEP_CURRENT` 与 `STOP_CHECKING` 使用相同条件，仅更新 `fallbackPolicyCount/fallbackCheckAt/version`，不增加 `groupFallbackCount`、不写切组日志、不移动队列。

`FAIL` 及策略错误使用专用 `casFallbackWaitingToFailed` SQL，条件与上述 CAS 一致，更新 `status='FAILED'`、`finish_time`、`error_code/error_msg`、`fallback_check_at=null`和 `version`。该 CAS 与下游依赖状态更新必须位于同一 `TransactionTemplate` 事务中；CAS miss 不得传播终态或清理 Redis。

`fallbackPolicyCount` 只在 Handler 确实开始调用且本次决策/策略错误 CAS 成功落库时增加。Handler 缺失或 executor 在回调开始前拒绝时不增加；回调超时、抛异常或返回非法决策且终态 CAS 成功时增加。

executor 拒绝使用专用 `deferFallbackAfterExecutorReject` CAS：

```sql
update scheduler_task
   set fallback_check_at=?,
       update_time=now(),
       version=version+1
 where id=?
   and status in ('PENDING','RUNNABLE','WAIT_RETRY')
   and group_code=?
   and fallback_check_at=?
   and version=?
   and (wait_deadline_at is null or wait_deadline_at > now());
```

退避时间为 `now + fallbackExecutorRejectBackoffMs`，不增加 `fallbackPolicyCount/groupFallbackCount`。若同时存在硬截止线，退避时间必须仍早于截止线；若已无法产生满足最小间隔且早于截止线的时间，则用相同快照条件清空 `fallback_check_at`，保持任务等待原硬超时 job 处理，不标记业务失败。CAS miss 仅记录指标。

### 7.3 与 Dispatcher 的竞争

当前 `casToRunning` 只按 taskId 和 status 更新。为了防止以下竞态，必须增强条件：

```text
Dispatcher读取A组任务并获取A并发
→ fallback CAS先把任务改到B
→ Dispatcher若只校验status，仍可能把数据库中的B任务置为RUNNING并按A配置执行
```

修改为：

```sql
update scheduler_task
   set status='RUNNING', ...
 where id=?
   and status in ('RUNNABLE','WAIT_RETRY')
   and group_code=?
   and version=?;
```

Dispatcher 使用读取快照中的 `groupCode/version`。若 fallback CAS 先成功，Dispatcher CAS 必须失败，并使用原 Group/executeNo 释放刚取得的并发。

Dispatcher 从 Group A 的 ready/user-ready 队列获取 ID 并读取 DB 快照后，必须在容量判断、Handler 查找和并发预占之前，校验 `task.groupCode == scannedGroupCode` 且 `task.dispatchRoute == scannedDispatchRoute`。不一致时仅使用当前队列坐标清理 A 中的残留成员，不得使用 DB 新快照中的 Group 构造删除 key，也不得预占 A 的并发。

## 8. Redis 队列迁移

数据库是状态真相，顺序固定为：

1. DB CAS + 切组日志事务提交。
2. 使用旧任务快照删除旧 Group 的 time queue 成员。
3. 使用旧任务快照删除旧 Group 的 ready/user-ready 成员，并重新平衡 active user。
4. 重新读取数据库任务。
5. 按新 `groupCode/status/executeAt` 决定是否写入新队列。

`QueueRedisService` 新增显式坐标 API，避免数据库快照已切组后删错队列：

```java
void removeFromReadyQueue(
        String queueGroupCode,
        String dispatchRoute,
        String userId,
        long taskId);

void removeQueueReferences(SchedulerTask oldSnapshot);
```

`removeQueueReferences` 删除旧 time queue 和旧 user-ready 成员，然后对旧 `groupCode/dispatchRoute/userId` 执行 active-user rebalance。Dispatcher 处理 stale member 时使用第一个显式 API，参数来自当前正在扫描的队列，而不是 DB 新快照。

状态处理：

- `RUNNABLE/WAIT_RETRY` 且已到 executeAt：加入新 Group ready 队列。
- `RUNNABLE/WAIT_RETRY` 且未到 executeAt：加入新 Group time 队列。
- `PENDING`：保持 PENDING，不强制加入 ready；由依赖满足逻辑或 queue refill 按最新 Group 恢复。

目标 Group 的组并发和用户并发不在切组时预占。任务真正被 Dispatcher 调度时重新计算；若当前用户在目标 Group 已达到并发上限，任务继续留在队列中等待，不影响该用户正在运行的其他任务。

DB 成功但 Redis 操作失败时，queue refill 按数据库中的新 Group 恢复。旧队列残留任务被读取时，Dispatcher 必须校验 DB Group 与当前扫描 Group，一旦不一致只清理旧记录，不执行任务。

`FAIL`、Handler 缺失、非法决策和回调异常/超时，在终态数据库事务成功后调用 `removeQueueReferences(oldSnapshot)`。fallback scan 或独立 wait-timeout job 处理硬超时时，由增强后的 `TaskStateService.markFailedByWaitDeadline` 使用事务内锁定的 `terminalSourceSnapshot` 清理，不使用调用前的过期快照。这些终态分支不写入任何新 Group 队列，仅入队依赖传播产生的下游任务。executor 拒绝不是终态，不清理现有队列。Redis 清理失败必须记录指标和结构化日志；stale member 后续由 Dispatcher 自清理。

## 9. WAIT_HOLD、重试、依赖与取消

- `WAIT_HOLD`：明确排除，因为它仍持有 Group/User running 计数。
- `WAIT_RETRY`：允许切组，但不增加 retryCount；切组与错误重试是两个独立维度。
- 依赖：PENDING 切组不改变依赖关系；依赖满足后按最新 Group 入队。
- 降级终态：FAIL 及所有策略错误都通过统一终态事务调用 `onUpstreamTaskTerminal(FAILED)`，不得绕过下游依赖传播。
- 取消：取消和降级 CAS 竞争，只有一个成功；CAS miss 后不得迁移 Redis。
- 硬截止：wait timeout 优先，ROUTE CAS 必须包含未过期条件。
- 业务幂等：本次改造不新增运行次数，但原框架仍保持 at-least-once 语义。

## 10. 错误码

| 错误码 | 场景 |
|--------|------|
| `SCHEDULE_WAIT_TIMEOUT` | 任务硬截止时间到期 |
| `FALLBACK_HANDLER_NOT_FOUND` | 当前 route 找不到任务 bizType 对应 Handler |
| `FALLBACK_DECISION_INVALID` | 决策为空、字段组合非法或时间非法 |
| `FALLBACK_TARGET_INVALID` | ROUTE 目标为空、等于当前 Group 或不存在 |
| `FALLBACK_TARGET_DISABLED` | 目标 Group 已禁用 |
| `FALLBACK_POLICY_EXCEPTION` | onGroupWaitTimeout 抛出异常 |
| `FALLBACK_POLICY_TIMEOUT` | onGroupWaitTimeout 超过配置时限 |
| `FALLBACK_POLICY_REJECTED` | fallback 专用 executor 已饱和；仅记录事件并执行非终态退避 |
| `SYNC_FALLBACK_CHECK_UNSUPPORTED` | executeSync 传入 fallbackCheckAt |

策略异常和非法决策首期将任务标记 FAILED 并清空 `fallbackCheckAt`，防止热循环。CAS miss 不是业务失败，只记录指标并结束本轮。

## 11. 配置项

```yaml
utask:
  scheduler:
    fallback-scan-interval-ms: 1000
    fallback-scan-limit: 200
    fallback-min-next-check-delay-ms: 1000
    fallback-policy-timeout-ms: 3000
    fallback-callback-threads: 4
    fallback-callback-queue-capacity: 0
    fallback-executor-reject-backoff-ms: 5000
```

配置启动校验：`fallbackCallbackThreads >= 1`、`fallbackCallbackQueueCapacity == 0`、`fallbackPolicyTimeoutMs >= 1`，且 `fallbackExecutorRejectBackoffMs >= fallbackMinNextCheckDelayMs`。

Fallback scan 使用独立分布式 job lock，锁名包含规范化后的 `dispatchRoute`。扫描必须按实例 `dispatchRoute` 隔离，避免错误服务调用不属于自己的 Handler。该锁只是减少重复回调的效率优化，不是正确性边界；锁过期后的重复计算仍由快照 CAS 保证单次生效。fallback 定时方法显式使用独立 `fallbackTaskScheduler`，回调显式使用 `fallbackCallbackExecutor`。

## 12. 模块改动

| 模块/文件 | 改动 |
|-----------|------|
| `TaskSubmitRequest` / `BatchSubmitTaskRequest` | 增加 fallbackCheckAt |
| `SchedulerTask` | 增加检查时间、策略次数和实际切组次数 |
| `TaskHandler` | 增加兼容的 onGroupWaitTimeout default 方法 |
| 新模型 | GroupFallbackDecision、GroupFallbackAction |
| `TaskRepository` / `JdbcTaskRepository` | 扫描、策略 CAS、切组日志写入、增强 casToRunning 条件 |
| `GroupConfigRepository` | 复用 findEnabledByGroupCode 校验目标 Group |
| 新 `GroupFallbackService` | 校验、切组/终态 CAS、依赖传播事务和事后队列处理 |
| `TaskStateService` / `TaskDependencyService` | 硬超时事务内锁定实际快照并清理队列，为 fallback 条件终态 CAS 提供依赖传播编排 |
| `SchedulerJobs` / `SchedulerJobRunner` | 增加 fallback scan 定时任务和 route 锁 |
| `SchedulerAutoConfiguration` | 提供独立有界 fallback callback executor 和专用 TaskScheduler |
| `DispatchService` | 传入 expected group/version，校验 DB Group 与队列 Group |
| `QueueRedisService` | 复用并补齐旧 time/ready/user-ready 清理能力 |
| SQL / README / demo | 迁移 SQL、示例和行为说明 |

明确不修改：`TaskExecuteResult`、`WorkerService` 结果分支、`ConcurrencyGuard`、`scheduler_task_execution`。

## 13. 实施步骤

### 阶段一：数据与 API

1. 添加主表字段、索引和切组日志表。
2. 扩展提交 DTO、SchedulerTask、JDBC insert/map。
3. 添加 Handler default 方法及决策模型。
4. 保证旧调用和原测试全部通过。

### 阶段二：扫描与数据库迁移

1. 实现按 route 的到期扫描和定时任务。
2. 实现 Handler 固定并发槽位隔离回调、开始时超时计时、拒绝非终态退避、决策校验、事务 CAS 和日志。
3. 实现 fallback 条件终态 CAS，并在同一事务内传播下游依赖。
4. 增强 Dispatcher `casToRunning` 的 expected group/version 条件。

### 阶段三：Redis 迁移与恢复

1. 实现旧 Group 队列显式坐标精确清理，包括切组和终态分支。
2. 按状态写入新 Group 队列。
3. 改造 wait-timeout 终态编排，验证始终按事务内实际快照清理队列。
4. 验证 DB 成功、Redis 失败后的 refill 恢复。

### 阶段四：测试与灰度

1. 单元测试、MySQL/Redis 集成测试和多实例竞争测试。
2. 更新 README、demo schema 和升级 SQL。
3. 先部署兼容版本，再灰度开启 fallback scan。

## 14. 测试与验收

### 14.1 基本行为

- 未传 fallbackCheckAt：行为与旧版本完全一致。
- A 到期后 ROUTE 到 B：数据库改为 B，旧队列清除，只进入 B 队列。
- KEEP_CURRENT(T2)：不切组，T2 前不再次回调。
- STOP_CHECKING：清空检查时间，任务继续正常等待。
- FAIL：任务终止，清理旧队列，不写新 Group 队列，下游依赖按 FAILED 正确解锁或终止。
- maxWaitSec 先到：直接失败，Handler 不调用。

### 14.2 并发与容量

- 两实例同时扫描：Handler 可被重复计算，但只有一次 CAS/日志/切组生效。
- Dispatcher 与降级竞争：两种 CAS 只有一个成功；失败方正确清理已取得资源。
- Handler 回调期间 Dispatcher 先置 RUNNING：fallback 终态 CAS 失败，不得误杀运行任务。
- A 的 ready 队列读到 DB 已属于 B 的任务：只删 A 成员，不删 B 成员，不预占 A 并发。
- 目标 Group 用户并发已满：任务保留在目标队列，待容量释放后执行。
- 同一用户其他 RUNNING 任务的计数不受切组影响。
- DB 切组后 Redis 写入失败：queue refill 最终恢复到新 Group。
- Handler 永不返回：超时后任务按明确错误码走条件终态 CAS，dispatch/recovery/refill 不受阻塞。
- Handler 忽略中断并占满 fallback executor：后续任务只退避 `fallbackCheckAt`，不增加策略次数，不进入业务终态。
- 并发槽位：同时运行回调不超过 `fallbackCallbackThreads`，timeout 从 Handler 实际 `startedAt` 计算，不从提交时计算。

### 14.3 状态边界

- RUNNING、WAIT_HOLD、终态任务不会被扫描或调用 Handler。
- 任务从等待态进入 SUCCESS/FAILED/CANCELLED 后，即使保留历史检查时间也永不再调用降级接口；fallback 产生的终态会主动清空该时间。
- WAIT_RETRY 可切组，但 retryCount 不变化。
- PENDING 依赖任务切组后，依赖满足时进入新 Group。
- executeSync 传 fallbackCheckAt 时明确失败，其他同步行为不变。
- FAIL/策略异常作为上游终态时，依赖 FAILED/TERMINAL 的下游任务正确入队，目标状态已不可能满足的依赖正确终止。
- FAIL/硬超时早于 executeAt 发生时，time queue 不留长期脏成员。
- fallback scan 读取 A/v1 后另一实例先 ROUTE 到 B/v2，硬超时随后成功：必须清理事务内锁定的 B 队列，不得只清理过期快照 A。
- 用 MySQL `EXPLAIN` 验证 null/non-null route 扫描均命中预期复合索引。

## 15. 监控指标

- `utask_fallback_policy_scan_total`
- `utask_fallback_policy_callback_total{bizType,action}`
- `utask_group_fallback_total{source,target}`
- `utask_group_fallback_cas_miss_total`
- `utask_group_fallback_failure_total{errorCode}`
- `utask_fallback_policy_duration_ms{bizType}`
- `utask_fallback_policy_timeout_total{bizType}`
- `utask_fallback_policy_rejected_total`
- `utask_fallback_redis_cleanup_failure_total{queueType}`
- `utask_fallback_stale_queue_member_total{group,route}`
- `utask_fallback_overdue_tasks`

日志包含 `taskId/taskNo/bizType/status/sourceGroup/targetGroup/fallbackCheckAt/fallbackCount`，不得记录完整业务敏感参数。

## 16. 上线与回滚

1. 先执行只增字段、表和索引的向前兼容 SQL。
2. 部署支持新字段但默认关闭 fallback scan 的版本。
3. 灰度开启 scan，业务逐步传入 fallbackCheckAt。
4. 回滚时先关闭 scan、停止新增 fallbackCheckAt，再回滚应用。
5. 新字段和日志表保留，不在应用回滚时删除。

## 17. 工作量评估

整体预计 **4–6 个工作日**：

- 数据模型与 API：0.5–1 天。
- 扫描、隔离回调、决策和数据库 CAS：1.5–2 天。
- 终态依赖传播、Redis 队列迁移与 Dispatcher 竞态修正：1.5–2 天。
- 测试、文档和灰度：0.5–1 天。

主要风险是 Dispatcher 与降级扫描竞争、DB/Redis 双写窗口以及业务回调副作用。通过 expected group/version CAS、条件终态事务与依赖传播、DB 作为真相、显式旧队列坐标清理、queue refill、独立有界回调 executor 和超时约束控制。

---
