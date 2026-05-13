package org.dong.scheduler.core.model.batch;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import java.time.LocalDateTime;
import java.util.List;

/**
 * 批量提交中的单个任务参数。
 *
 * <p>默认值：
 * <ul>
 *   <li>groupCode: 未传时回退到 scheduler.default-group-code</li>
 *   <li>executeAt: 当前时间（立即执行）</li>
 *   <li>maxRetryCount: 3</li>
 *   <li>priority: 0</li>
 * </ul>
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Accessors(chain = true)
public class BatchSubmitTaskRequest {
    /**
     * 批内任务引用ID，批次内唯一（必填）。
     */
    private String clientTaskRef;
    /**
     * 任务组编码（可选，未传时回退到 scheduler.default-group-code）。
     */
    private String groupCode;
    /**
     * 用户ID（必填）。
     */
    private String userId;
    /**
     * 业务类型（必填，对应 TaskHandler#bizType）。
     */
    private String bizType;
    /**
     * 业务键（必填，可重复；是否幂等由业务侧自行控制）。
     */
    private String bizKey;
    /**
     * 优先级，值越大越优先，默认 0（有效范围 0~99999，超出会被自动截断）。
     */
    private Integer priority;
    /**
     * 计划执行时间，默认当前时间（立即执行）。
     */
    private LocalDateTime executeAt;
    /**
     * 最大重试次数，默认 3。
     */
    private Integer maxRetryCount;
    /**
     * 单任务执行超时（秒），可选。
     */
    private Integer executeTimeoutSec;
    /**
     * 单任务重试间隔（秒），可选；为空时使用全局 defaultRetryDelaySec。
     */
    private Integer retryDelaySec;
    /**
     * 扩展信息（字符串），可选。
     */
    private String extInfo;
    /**
     * 依赖任务列表（可选）。
     *
     * <p>只有所有依赖都满足各自目标状态后当前任务才可执行。
     */
    private List<BatchSubmitDependencyRequest> dependencies;
}
