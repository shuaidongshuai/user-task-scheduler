package org.dong.scheduler.autoconfigure;

import org.dong.scheduler.config.SchedulerProperties;
import org.dong.scheduler.core.job.SchedulerJobs;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class SchedulerJobRunnerTest {
    @Test
    void shouldRunFallbackScanByDefault() {
        SchedulerProperties properties = new SchedulerProperties();
        SchedulerJobs jobs = mock(SchedulerJobs.class);

        new SchedulerJobRunner(properties, jobs).scanGroupFallback();

        verify(jobs).scanGroupFallback();
    }

    @Test
    void shouldRunFallbackScanOnlyWhenDispatchAndFallbackAreEnabled() {
        SchedulerProperties properties = new SchedulerProperties();
        properties.setFallbackEnabled(false);
        SchedulerJobs jobs = mock(SchedulerJobs.class);
        SchedulerJobRunner runner = new SchedulerJobRunner(properties, jobs);

        runner.scanGroupFallback();
        verify(jobs, never()).scanGroupFallback();

        properties.setFallbackEnabled(true);
        runner.scanGroupFallback();
        properties.setDispatchEnabled(false);
        runner.scanGroupFallback();

        verify(jobs).scanGroupFallback();
    }
}
