package org.dong.demo.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.dong.demo.repo.DemoBizTaskRepository;
import org.dong.scheduler.core.model.SchedulerTask;
import org.dong.scheduler.core.model.TaskExecuteResult;
import org.dong.scheduler.core.model.GroupFallbackDecision;
import org.dong.scheduler.core.spi.TaskHandler;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Component
@Slf4j
public class DemoTaskHandler implements TaskHandler {
    private final DemoBizTaskRepository demoBizTaskRepository;
    private final ObjectMapper objectMapper;

    public DemoTaskHandler(DemoBizTaskRepository demoBizTaskRepository, ObjectMapper objectMapper) {
        this.demoBizTaskRepository = demoBizTaskRepository;
        this.objectMapper = objectMapper;
    }

    @Override
    public List<String> bizTypes() {
        return List.of("demo.biz.process", "demo.biz.process.v2");
    }

    @Override
    public TaskExecuteResult execute(SchedulerTask task) throws Exception {
        String bizKey = task.getBizKey();
        if (bizKey == null || bizKey.isBlank()) {
            return TaskExecuteResult.failed("MISSING_BIZ_KEY", "bizKey is required", false);
        }

        Map<String, Object> ext = parseExt(task.getExtInfo());
        DemoExecutionOptions options = parseOptions(ext);
        int failBeforeSuccess = options.failBeforeSuccess == null ? 2 : Math.max(0, options.failBeforeSuccess);
        int waitHoldRoundsBeforeSuccess = options.waitHoldRoundsBeforeSuccess == null
                ? 0
                : Math.max(0, options.waitHoldRoundsBeforeSuccess);
        long sleepMs = options.sleepMs == null ? 2_000L : Math.max(0L, options.sleepMs);

        if (waitHoldRoundsBeforeSuccess > 0 && task.getHoldRoundCount() < waitHoldRoundsBeforeSuccess) {
            demoBizTaskRepository.updateStatus(bizKey, "POLLING");
            ext.put("phase", "POLLING");
            ext.put("observedHoldRound", task.getHoldRoundCount() + 1);
            task.setExtInfo(writeExt(ext));
            log.info("task wait hold, taskId={}, bizKey={}, holdRound={}/{}",
                    task.getId(), bizKey, task.getHoldRoundCount() + 1, waitHoldRoundsBeforeSuccess);
            return TaskExecuteResult.waitHold();
        }

        if (task.getRetryCount() < failBeforeSuccess) {
            demoBizTaskRepository.updateStatus(bizKey, "RUNNING");
            ext.put("last_retry_count", task.getRetryCount() + 1);
            task.setExtInfo(writeExt(ext));
            log.info("simulate retryable fail before success, taskId={}, bizKey={}, retryCount={}",
                    task.getId(), bizKey, task.getRetryCount());
            if (options.executeRetryTargetGroup != null && !options.executeRetryTargetGroup.isBlank()) {
                return TaskExecuteResult.retryableOnGroup("RETRYABLE_FAIL", "simulated retry before success",
                        options.executeRetryTargetGroup.trim(), null);
            }
            return TaskExecuteResult.failed("RETRYABLE_FAIL", "simulated retry before success", true);
        }

        demoBizTaskRepository.updateStatus(bizKey, "RUNNING");
        Thread.sleep(sleepMs);
        demoBizTaskRepository.updateStatus(bizKey, "SUCCESS");
        ext.put("phase", "SUCCESS");
        task.setExtInfo(writeExt(ext));
        log.info("task success, taskId={}, bizKey={}, extInfo={}", task.getId(), bizKey, task.getExtInfo());
        return TaskExecuteResult.success();
    }

    @Override
    public GroupFallbackDecision onGroupWaitTimeout(SchedulerTask task) {
        Map<String, Object> ext = parseExt(task.getExtInfo());
        Object target = ext.get("fallbackTargetGroup");
        if (target == null || String.valueOf(target).isBlank()) {
            return GroupFallbackDecision.stopChecking();
        }
        return GroupFallbackDecision.routeTo(String.valueOf(target).trim(), null);
    }

    private Map<String, Object> parseExt(String extInfo) {
        if (extInfo == null || extInfo.isBlank()) {
            return new LinkedHashMap<>();
        }
        try {
            return objectMapper.readValue(extInfo, objectMapper.getTypeFactory().constructMapType(LinkedHashMap.class, String.class, Object.class));
        } catch (Exception ex) {
            log.warn("parse extInfo failed, fallback to default demo behavior, extInfo={}", extInfo, ex);
            return new LinkedHashMap<>();
        }
    }

    private DemoExecutionOptions parseOptions(Map<String, Object> ext) {
        return objectMapper.convertValue(ext, DemoExecutionOptions.class);
    }

    private String writeExt(Map<String, Object> ext) {
        try {
            return objectMapper.writeValueAsString(ext);
        } catch (Exception ex) {
            log.warn("write extInfo failed, fallback to empty json, ext={}", ext, ex);
            return "{}";
        }
    }

    private static final class DemoExecutionOptions {
        private Integer failBeforeSuccess;
        private Integer waitHoldRoundsBeforeSuccess;
        private Long sleepMs;
        private String executeRetryTargetGroup;

        public Integer getFailBeforeSuccess() {
            return failBeforeSuccess;
        }

        public void setFailBeforeSuccess(Integer failBeforeSuccess) {
            this.failBeforeSuccess = failBeforeSuccess;
        }

        public Long getSleepMs() {
            return sleepMs;
        }

        public void setSleepMs(Long sleepMs) {
            this.sleepMs = sleepMs;
        }

        public String getExecuteRetryTargetGroup() {
            return executeRetryTargetGroup;
        }

        public void setExecuteRetryTargetGroup(String executeRetryTargetGroup) {
            this.executeRetryTargetGroup = executeRetryTargetGroup;
        }

        public Integer getWaitHoldRoundsBeforeSuccess() {
            return waitHoldRoundsBeforeSuccess;
        }

        public void setWaitHoldRoundsBeforeSuccess(Integer waitHoldRoundsBeforeSuccess) {
            this.waitHoldRoundsBeforeSuccess = waitHoldRoundsBeforeSuccess;
        }
    }
}
