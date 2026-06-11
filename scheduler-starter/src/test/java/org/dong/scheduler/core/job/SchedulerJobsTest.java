package org.dong.scheduler.core.job;

import org.dong.scheduler.core.model.GroupConfig;
import org.dong.scheduler.core.repo.GroupConfigRepository;
import org.dong.scheduler.core.service.DispatchService;
import org.dong.scheduler.core.service.RecoveryService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SchedulerJobsTest {

    @Mock
    private DispatchService dispatchService;
    @Mock
    private RecoveryService recoveryService;
    @Mock
    private GroupConfigRepository groupConfigRepository;

    @Test
    void shouldWrapRecoverWithUnifiedScheduledJobLock() {
        SchedulerJobs schedulerJobs = new SchedulerJobs(dispatchService, recoveryService, groupConfigRepository);
        GroupConfig groupConfig = new GroupConfig();
        groupConfig.setGroupCode("g1");
        groupConfig.setHeartbeatTimeoutSec(90);

        when(groupConfigRepository.listEnabled()).thenReturn(List.of(groupConfig));
        doAnswer(invocation -> {
                    Runnable work = invocation.getArgument(2);
                    work.run();
                    return null;
                })
                .when(recoveryService)
                .runWithScheduledJobLock(eq("recover"), eq("recovery scan"), any(Runnable.class));
        when(recoveryService.recoverTimeoutRunning("g1", 90)).thenReturn(2);
        when(recoveryService.reconcileRunningCountersIfNeeded(List.of("g1"))).thenReturn(1);

        schedulerJobs.recover();

        verify(recoveryService).runWithScheduledJobLock(eq("recover"), eq("recovery scan"), any(Runnable.class));
        verify(recoveryService).recoverTimeoutRunning("g1", 90);
        verify(recoveryService).reconcileRunningCountersIfNeeded(List.of("g1"));
    }

    @Test
    void shouldSkipRecoverWhenUnifiedScheduledJobLockBusy() {
        SchedulerJobs schedulerJobs = new SchedulerJobs(dispatchService, recoveryService, groupConfigRepository);
        doNothing().when(recoveryService)
                .runWithScheduledJobLock(eq("recover"), eq("recovery scan"), any(Runnable.class));

        schedulerJobs.recover();

        verify(recoveryService).runWithScheduledJobLock(eq("recover"), eq("recovery scan"), any(Runnable.class));
        verify(groupConfigRepository, never()).listEnabled();
        verify(recoveryService, never()).recoverTimeoutRunning(anyString(), anyInt());
        verify(recoveryService, never()).reconcileRunningCountersIfNeeded(any());
    }
}
