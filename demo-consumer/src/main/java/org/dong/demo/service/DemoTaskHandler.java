package org.dong.demo.service;

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

    public DemoTaskHandler(DemoBizTaskRepository demoBizTaskRepository) {
        this.demoBizTaskRepository = demoBizTaskRepository;
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

        if (task.getRetryCount() < 2) {
            demoBizTaskRepository.updateStatus(bizKey, "RUNNING");
            String nextExtInfo = "{\"last_retry_count\":" + (task.getRetryCount() + 1) + "}";
            task.setExtInfo(nextExtInfo);
            log.info("simulate retryable fail before success, taskId={}, bizKey={}, retryCount={}",
                    task.getId(), bizKey, task.getRetryCount());
            return TaskExecuteResult.failed("RETRYABLE_FAIL", "simulated retry before success", true);
        }

        Thread.sleep(2000L);
        demoBizTaskRepository.updateStatus(bizKey, "SUCCESS");
        log.info("task success, taskId={}, bizKey={}, extInfo={}", task.getId(), bizKey, task.getExtInfo());
        return TaskExecuteResult.success();
    }
}
