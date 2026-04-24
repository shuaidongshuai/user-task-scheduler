package org.dong.demo.service;

import org.dong.demo.repo.DemoBizTaskRepository;
import org.dong.scheduler.core.model.SchedulerTask;
import org.dong.scheduler.core.model.TaskExecuteResult;
import org.dong.scheduler.core.spi.TaskHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class DemoTaskHandler implements TaskHandler {
    private static final Logger log = LoggerFactory.getLogger(DemoTaskHandler.class);

    private final DemoBizTaskRepository demoBizTaskRepository;

    public DemoTaskHandler(DemoBizTaskRepository demoBizTaskRepository) {
        this.demoBizTaskRepository = demoBizTaskRepository;
    }

    @Override
    public String bizType() {
        return "demo.biz.process";
    }

    @Override
    public TaskExecuteResult execute(SchedulerTask task) throws Exception {
        String bizKey = task.getBizKey();
        if (bizKey == null || bizKey.isBlank()) {
            return TaskExecuteResult.failed("MISSING_BIZ_KEY", "bizKey is required", false);
        }

        if (task.getRetryCount() < 2) {
            demoBizTaskRepository.updateStatus(bizKey, "RUNNING");
            String nextExtInfo = "{\"last_retry_count\":" + (task.getRetryCount() + 1) + "}";
            log.info("simulate retryable fail before success, taskId={}, bizKey={}, retryCount={}",
                    task.getId(), bizKey, task.getRetryCount());
            return TaskExecuteResult.failed("RETRYABLE_FAIL", "simulated retry before success", true, nextExtInfo);
        }

        Thread.sleep(2000L);
        demoBizTaskRepository.updateStatus(bizKey, "SUCCESS");
        log.info("task success, taskId={}, bizKey={}, extInfo={}", task.getId(), bizKey, task.getExtInfo());
        return TaskExecuteResult.success(task.getExtInfo());
    }
}
