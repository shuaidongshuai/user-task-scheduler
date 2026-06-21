package org.dong.demo.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.dong.demo.repo.DemoBizTaskRepository;
import org.dong.scheduler.core.model.SchedulerTask;
import org.dong.scheduler.core.model.TaskExecuteResult;
import org.dong.scheduler.core.spi.TaskHandler;
import org.springframework.stereotype.Component;

import java.util.List;

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

        DemoExecutionOptions options = parseOptions(task.getExtInfo());
        int failBeforeSuccess = options.failBeforeSuccess == null ? 2 : Math.max(0, options.failBeforeSuccess);
        long sleepMs = options.sleepMs == null ? 2_000L : Math.max(0L, options.sleepMs);

        if (task.getRetryCount() < failBeforeSuccess) {
            demoBizTaskRepository.updateStatus(bizKey, "RUNNING");
            String nextExtInfo = "{\"last_retry_count\":" + (task.getRetryCount() + 1) + "}";
            task.setExtInfo(nextExtInfo);
            log.info("simulate retryable fail before success, taskId={}, bizKey={}, retryCount={}",
                    task.getId(), bizKey, task.getRetryCount());
            return TaskExecuteResult.failed("RETRYABLE_FAIL", "simulated retry before success", true);
        }

        demoBizTaskRepository.updateStatus(bizKey, "RUNNING");
        Thread.sleep(sleepMs);
        demoBizTaskRepository.updateStatus(bizKey, "SUCCESS");
        log.info("task success, taskId={}, bizKey={}, extInfo={}", task.getId(), bizKey, task.getExtInfo());
        return TaskExecuteResult.success();
    }

    private DemoExecutionOptions parseOptions(String extInfo) {
        if (extInfo == null || extInfo.isBlank()) {
            return new DemoExecutionOptions();
        }
        try {
            return objectMapper.readValue(extInfo, DemoExecutionOptions.class);
        } catch (Exception ex) {
            log.warn("parse extInfo failed, fallback to default demo behavior, extInfo={}", extInfo, ex);
            return new DemoExecutionOptions();
        }
    }

    private static final class DemoExecutionOptions {
        private Integer failBeforeSuccess;
        private Long sleepMs;

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
    }
}
