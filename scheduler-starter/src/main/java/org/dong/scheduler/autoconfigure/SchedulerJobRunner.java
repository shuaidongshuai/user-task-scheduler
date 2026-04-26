package org.dong.scheduler.autoconfigure;

import org.dong.scheduler.config.SchedulerProperties;
import org.dong.scheduler.core.job.SchedulerJobs;
import org.dong.scheduler.core.util.ThreadContextUtil;
import org.springframework.scheduling.annotation.Scheduled;

public class SchedulerJobRunner {
    private final SchedulerProperties properties;
    private final SchedulerJobs jobs;

    public SchedulerJobRunner(SchedulerProperties properties, SchedulerJobs jobs) {
        this.properties = properties;
        this.jobs = jobs;
    }

    @Scheduled(fixedDelayString = "${scheduler.dispatch-interval-ms:500}")
    public void dispatch() {
        if (!properties.isEnabled()) {
            return;
        }
        ThreadContextUtil.addNewContext(jobs::dispatch).run();
    }

    @Scheduled(fixedDelayString = "${scheduler.recovery-interval-ms:30000}")
    public void recover() {
        if (!properties.isEnabled()) {
            return;
        }
        ThreadContextUtil.addNewContext(jobs::recover).run();
    }

    @Scheduled(fixedDelayString = "${scheduler.queue-refill-interval-ms:15000}")
    public void refillQueue() {
        if (!properties.isEnabled()) {
            return;
        }
        ThreadContextUtil.addNewContext(jobs::refillQueue).run();
    }
}
