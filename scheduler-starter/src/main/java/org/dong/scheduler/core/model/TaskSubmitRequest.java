package org.dong.scheduler.core.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import java.time.LocalDateTime;

/**
 * 任务提交参数。
 *
 * <p>默认值：
 * <ul>
 *   <li>executeAt: 当前时间（立即执行）</li>
 *   <li>maxRetryCount: 3</li>
 *   <li>priority: 0</li>
 * </ul>
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Accessors(chain = true)
public class TaskSubmitRequest {
    /**
     * 任务组编码（必填）。
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
     * 业务幂等键（必填，和 bizType 组成提交幂等键）。
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
}
