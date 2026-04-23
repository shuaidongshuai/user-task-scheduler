package org.dong.scheduler.autoconfigure;

import org.dong.scheduler.config.SchedulerProperties;
import org.dong.scheduler.core.job.SchedulerJobs;
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
        jobs.dispatch();
    }

    @Scheduled(fixedDelayString = "${scheduler.recovery-interval-ms:30000}")
    public void recover() {
        if (!properties.isEnabled()) {
            return;
        }
        jobs.recover();
    }

    @Scheduled(fixedDelayString = "${scheduler.queue-refill-interval-ms:15000}")
    public void refillQueue() {
        if (!properties.isEnabled()) {
            return;
        }
        jobs.refillQueue();
    }
}
