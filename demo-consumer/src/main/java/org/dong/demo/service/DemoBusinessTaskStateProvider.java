package org.dong.demo.service;

import org.dong.demo.repo.DemoBizTaskRepository;
import org.dong.scheduler.core.enums.BusinessTaskState;
import org.dong.scheduler.core.model.SchedulerTask;
import org.dong.scheduler.core.spi.BusinessTaskStateProvider;
import org.springframework.stereotype.Component;

@Component
public class DemoBusinessTaskStateProvider implements BusinessTaskStateProvider {
    private final DemoBizTaskRepository repository;

    public DemoBusinessTaskStateProvider(DemoBizTaskRepository repository) {
        this.repository = repository;
    }

    @Override
    public BusinessTaskState query(SchedulerTask task) {
        if (task.getBizKey() == null || task.getBizKey().isBlank()) {
            return BusinessTaskState.UNKNOWN;
        }
        return repository.findByBizKey(task.getBizKey())
                .map(t -> {
                    if ("SUCCESS".equalsIgnoreCase(t.getStatus())) {
                        return BusinessTaskState.SUCCESS;
                    } else if ("FAILED".equalsIgnoreCase(t.getStatus())) {
                        return BusinessTaskState.FAILED;
                    } else if ("RUNNING".equalsIgnoreCase(t.getStatus())) {
                        return BusinessTaskState.RUNNING;
                    }
                    return BusinessTaskState.NEED_RUNNING;
                })
                .orElse(BusinessTaskState.UNKNOWN);
    }
}
