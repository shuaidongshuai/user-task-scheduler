package org.dong.scheduler.core.job;

import lombok.extern.slf4j.Slf4j;
import org.dong.scheduler.core.model.GroupConfig;
import org.dong.scheduler.core.repo.GroupConfigRepository;
import org.dong.scheduler.core.service.DispatchService;
import org.dong.scheduler.core.service.RecoveryService;

@Slf4j
public class SchedulerJobs {
    private final DispatchService dispatchService;
    private final RecoveryService recoveryService;
    private final GroupConfigRepository groupConfigRepository;

    /**
     * 调度任务Jobs
     * <p>
     * 提供定时任务调度、任务恢复和队列填充等核心功能。
     *
     * @param dispatchService       调度服务
     * @param recoveryService       恢复服务
     * @param groupConfigRepository 分组配置仓储
     */
    public SchedulerJobs(DispatchService dispatchService,
                         RecoveryService recoveryService,
                         GroupConfigRepository groupConfigRepository) {
        this.dispatchService = dispatchService;
        this.recoveryService = recoveryService;
        this.groupConfigRepository = groupConfigRepository;
    }

    /**
     * 执行一次任务调度
     * <p>
     * 触发调度服务从待执行队列中获取任务并分发到执行器。
     */
    public void dispatch() {
        long begin = System.currentTimeMillis();
        log.debug("scheduler dispatch job start");
        dispatchService.dispatchOnce();
        log.debug("scheduler dispatch job end, costMs={}", System.currentTimeMillis() - begin);
    }

    /**
     * 恢复超时运行的任务
     * <p>
     * 遍历所有启用的任务组，对每个组检测超时未完成的任务，
     * 将其状态恢复为待执行，并重新放入调度队列。
     */
    public void recover() {
        long begin = System.currentTimeMillis();
        log.debug("scheduler recover job start");
        var groups = groupConfigRepository.listEnabled();
        int totalRecovered = 0;
        for (GroupConfig cfg : groups) {
            try {
                totalRecovered += recoveryService.recoverTimeoutRunning(cfg.getGroupCode(), cfg.getHeartbeatTimeoutSec());
            } catch (Exception e) {
                log.error("recover failed, group={}", cfg.getGroupCode(), e);
            }
        }
        int totalReconciledGroups = recoveryService.reconcileRunningCountersIfNeeded(
                groups.stream().map(GroupConfig::getGroupCode).toList()
        );
        if (totalRecovered > 0 || totalReconciledGroups > 0) {
            log.info("scheduler recover job end, recovered={}, reconciledGroups={}, costMs={}",
                    totalRecovered, totalReconciledGroups, System.currentTimeMillis() - begin);
        } else {
            log.debug("scheduler recover job end, recovered=0, reconciledGroups=0, costMs={}", System.currentTimeMillis() - begin);
        }
    }

    /**
     * 重新填充任务队列
     * <p>
     * 调用恢复服务将待恢复的任务重新填充到队列中，
     * 并根据填充数量记录相应级别的日志信息。
     */
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
