package org.dong.scheduler.core.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import java.time.LocalDateTime;
import java.util.Map;

/**
 * 任务提交参数。
 *
 * <p>默认值：
 * <ul>
 *   <li>executeAt: 当前时间（立即执行）</li>
 *   <li>maxRetryCount: 3</li>
 *   <li>priority: 0</li>
 *   <li>taskNo: 自动生成</li>
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
     * 业务幂等键，可选。
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
     * 扩展参数，可选。
     */
    private Map<String, Object> ext;
    /**
     * 任务唯一号；由调度器自动生成。
     */
    private String taskNo;
}
