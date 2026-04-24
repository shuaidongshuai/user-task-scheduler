package org.dong.demo.controller;

import org.dong.demo.domain.DemoBizTask;
import org.dong.demo.repo.DemoBizTaskRepository;
import org.dong.scheduler.core.model.TaskSubmitRequest;
import org.dong.scheduler.core.spi.SchedulerClient;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

@RestController
@RequestMapping("/demo")
public class DemoController {
    private final SchedulerClient schedulerClient;
    private final DemoBizTaskRepository bizTaskRepository;

    public DemoController(SchedulerClient schedulerClient, DemoBizTaskRepository bizTaskRepository) {
        this.schedulerClient = schedulerClient;
        this.bizTaskRepository = bizTaskRepository;
    }

    @PostMapping("/submit")
    public Map<String, Object> submit(@RequestBody SubmitRequest request) {
        String bizKey = request.bizKey() == null || request.bizKey().isBlank()
                ? "biz-" + UUID.randomUUID().toString().replace("-", "")
                : request.bizKey();

        bizTaskRepository.insert(bizKey, request.payload() == null ? "{}" : request.payload());

        TaskSubmitRequest submitRequest = new TaskSubmitRequest()
                .setGroupCode(request.groupCode() == null ? "demo-group" : request.groupCode())
                .setUserId(request.userId() == null ? "demo-user" : request.userId())
                .setBizType("demo.biz.process")
                .setBizKey(bizKey)
                .setPriority(request.priority() == null ? 50 : request.priority())
                .setExecuteAt(request.executeAt())
                .setMaxRetryCount(request.maxRetryCount())
                .setExecuteTimeoutSec(request.executeTimeoutSec() == null ? 1 : request.executeTimeoutSec())
                .setRetryDelaySec(request.retryDelaySec())
                .setExtInfo(request.extInfo() == null || request.extInfo().isBlank()
                        ? (request.forceRetry() != null && request.forceRetry() ? "{\"force_retry\":true}" : null)
                        : request.extInfo());
        long taskId = schedulerClient.submit(submitRequest);

        return Map.of("taskId", taskId, "bizKey", bizKey);
    }

    @PostMapping("/biz/{bizKey}/status/{status}")
    public Map<String, Object> updateBizStatus(@PathVariable String bizKey, @PathVariable String status) {
        bizTaskRepository.updateStatus(bizKey, status);
        return Map.of("bizKey", bizKey, "status", status);
    }

    @GetMapping("/biz/{bizKey}")
    public Optional<DemoBizTask> queryBiz(@PathVariable String bizKey) {
        return bizTaskRepository.findByBizKey(bizKey);
    }

    public record SubmitRequest(
            String groupCode,
            String userId,
            String bizKey,
            Integer priority,
            Integer maxRetryCount,
            Integer executeTimeoutSec,
            Integer retryDelaySec,
            LocalDateTime executeAt,
            String extInfo,
            String payload,
            Boolean forceRetry
    ) {
    }
}
