package org.dong.scheduler.core.job;

import org.dong.scheduler.core.model.GroupConfig;
import org.dong.scheduler.core.repo.GroupConfigRepository;
import org.dong.scheduler.core.service.DispatchService;
import org.dong.scheduler.core.service.RecoveryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchedulerJobs {
    private static final Logger log = LoggerFactory.getLogger(SchedulerJobs.class);

    private final DispatchService dispatchService;
    private final RecoveryService recoveryService;
    private final GroupConfigRepository groupConfigRepository;

    public SchedulerJobs(DispatchService dispatchService,
                         RecoveryService recoveryService,
                         GroupConfigRepository groupConfigRepository) {
        this.dispatchService = dispatchService;
        this.recoveryService = recoveryService;
        this.groupConfigRepository = groupConfigRepository;
    }

    public void dispatch() {
        long begin = System.currentTimeMillis();
        log.debug("scheduler dispatch job start");
        dispatchService.dispatchOnce();
        log.debug("scheduler dispatch job end, costMs={}", System.currentTimeMillis() - begin);
    }

    public void recover() {
        long begin = System.currentTimeMillis();
        log.debug("scheduler recover job start");
        int totalRecovered = 0;
        for (GroupConfig cfg : groupConfigRepository.listEnabled()) {
            try {
                totalRecovered += recoveryService.recoverTimeoutRunning(cfg.getGroupCode(), cfg.getHeartbeatTimeoutSec());
            } catch (Exception e) {
                log.error("recover failed, group={}", cfg.getGroupCode(), e);
            }
        }
        if (totalRecovered > 0) {
            log.info("scheduler recover job end, recovered={}, costMs={}", totalRecovered, System.currentTimeMillis() - begin);
        } else {
            log.debug("scheduler recover job end, recovered=0, costMs={}", System.currentTimeMillis() - begin);
        }
    }

    public void refillQueue() {
        long begin = System.currentTimeMillis();
        log.debug("scheduler refill job start");
        int refilled = recoveryService.refillQueue();
        if (refilled > 0) {
            log.info("scheduler refill job end, refilled={}, costMs={}", refilled, System.currentTimeMillis() - begin);
        } else {
            log.debug("scheduler refill job end, refilled=0, costMs={}", System.currentTimeMillis() - begin);
        }
    }
}
