package org.dong.scheduler.autoconfigure;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.dong.scheduler.config.SchedulerProperties;
import org.dong.scheduler.core.job.SchedulerJobs;
import org.dong.scheduler.core.redis.ConcurrencyGuard;
import org.dong.scheduler.core.redis.QueueRedisService;
import org.dong.scheduler.core.redis.RedisConcurrencyGuard;
import org.dong.scheduler.core.repo.GroupConfigRepository;
import org.dong.scheduler.core.repo.JdbcGroupConfigRepository;
import org.dong.scheduler.core.repo.JdbcTaskDependencyRepository;
import org.dong.scheduler.core.repo.JdbcTaskRepository;
import org.dong.scheduler.core.repo.TaskDependencyRepository;
import org.dong.scheduler.core.repo.TaskRepository;
import org.dong.scheduler.core.service.BusinessTaskStateProviderRegistry;
import org.dong.scheduler.core.service.DefaultTaskDependencyService;
import org.dong.scheduler.core.service.DefaultSchedulerClient;
import org.dong.scheduler.core.service.DispatchService;
import org.dong.scheduler.core.service.DynamicUserLimitService;
import org.dong.scheduler.core.service.GroupFallbackScanner;
import org.dong.scheduler.core.service.GroupFallbackService;
import org.dong.scheduler.core.service.RecoveryService;
import org.dong.scheduler.core.service.TaskDependencyService;
import org.dong.scheduler.core.service.TaskHandlerRegistry;
import org.dong.scheduler.core.service.TaskStateService;
import org.dong.scheduler.core.service.WorkerService;
import org.dong.scheduler.core.spi.BusinessTaskStateProvider;
import org.dong.scheduler.core.spi.SchedulerClient;
import org.dong.scheduler.core.spi.TaskHandler;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.net.InetAddress;
import java.util.UUID;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@AutoConfiguration
@EnableScheduling
@EnableConfigurationProperties(SchedulerProperties.class)
@ConditionalOnProperty(prefix = "utask.scheduler", name = "enabled", havingValue = "true", matchIfMissing = true)
@ConditionalOnClass({org.springframework.jdbc.core.JdbcTemplate.class, org.springframework.data.redis.core.StringRedisTemplate.class})
public class SchedulerAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public TaskRepository taskRepository(org.springframework.jdbc.core.JdbcTemplate jdbcTemplate) {
        return new JdbcTaskRepository(jdbcTemplate);
    }

    @Bean
    @ConditionalOnMissingBean
    public TaskDependencyRepository taskDependencyRepository(org.springframework.jdbc.core.JdbcTemplate jdbcTemplate) {
        return new JdbcTaskDependencyRepository(jdbcTemplate);
    }

    @Bean
    @ConditionalOnMissingBean
    public GroupConfigRepository groupConfigRepository(org.springframework.jdbc.core.JdbcTemplate jdbcTemplate) {
        return new JdbcGroupConfigRepository(jdbcTemplate);
    }

    @Bean
    @ConditionalOnMissingBean
    public QueueRedisService queueRedisService(org.springframework.data.redis.core.StringRedisTemplate redisTemplate,
                                               SchedulerProperties properties) {
        return new QueueRedisService(redisTemplate, properties);
    }

    @Bean
    @ConditionalOnMissingBean
    public ConcurrencyGuard concurrencyGuard(org.springframework.data.redis.core.StringRedisTemplate redisTemplate) {
        return new RedisConcurrencyGuard(redisTemplate);
    }

    @Bean
    @ConditionalOnMissingBean
    public DynamicUserLimitService dynamicUserLimitService(ObjectMapper objectMapper) {
        return new DynamicUserLimitService(objectMapper);
    }

    @Bean
    @ConditionalOnMissingBean
    public TransactionTemplate transactionTemplate(PlatformTransactionManager transactionManager) {
        return new TransactionTemplate(transactionManager);
    }

    @Bean
    @ConditionalOnMissingBean
    public TaskHandlerRegistry taskHandlerRegistry(ObjectProvider<TaskHandler> handlers) {
        return new TaskHandlerRegistry(handlers);
    }

    @Bean
    @ConditionalOnMissingBean(name = "schedulerWorkerExecutor")
    public ThreadPoolTaskExecutor schedulerWorkerExecutor(SchedulerProperties properties) {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setThreadNamePrefix("sched-worker-");
        executor.setCorePoolSize(properties.getWorkerThreads());
        executor.setMaxPoolSize(Math.max(properties.getWorkerThreads(), properties.getMaxWorkerThreads()));
        executor.setQueueCapacity(0);
        executor.initialize();
        return executor;
    }

    @Bean(name = "fallbackCallbackExecutor", destroyMethod = "shutdown")
    @ConditionalOnMissingBean(name = "fallbackCallbackExecutor")
    public ThreadPoolExecutor fallbackCallbackExecutor(SchedulerProperties properties) {
        validateFallbackProperties(properties);
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                properties.getFallbackCallbackThreads(),
                properties.getFallbackCallbackThreads(),
                0L,
                TimeUnit.MILLISECONDS,
                new SynchronousQueue<>(),
                runnable -> {
                    Thread thread = new Thread(runnable);
                    thread.setName("sched-fallback-callback-" + thread.threadId());
                    thread.setDaemon(true);
                    return thread;
                },
                new ThreadPoolExecutor.AbortPolicy()
        );
        executor.prestartAllCoreThreads();
        return executor;
    }

    @Bean(name = "fallbackTaskScheduler")
    @ConditionalOnMissingBean(name = "fallbackTaskScheduler")
    public ThreadPoolTaskScheduler fallbackTaskScheduler() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(1);
        scheduler.setThreadNamePrefix("sched-fallback-scan-");
        return scheduler;
    }

    @Bean
    @ConditionalOnMissingBean
    public BusinessTaskStateProviderRegistry businessTaskStateProviderRegistry(
            ObjectProvider<BusinessTaskStateProvider> providers) {
        return new BusinessTaskStateProviderRegistry(providers);
    }

    @Bean
    @ConditionalOnMissingBean
    public TaskDependencyService taskDependencyService(TaskRepository taskRepository,
                                                       TaskDependencyRepository taskDependencyRepository) {
        return new DefaultTaskDependencyService(taskRepository, taskDependencyRepository);
    }

    @Bean
    @ConditionalOnMissingBean
    public TaskStateService taskStateService(TaskRepository taskRepository,
                                             TaskDependencyService taskDependencyService,
                                             ConcurrencyGuard concurrencyGuard,
                                             QueueRedisService queueRedisService,
                                             TransactionTemplate transactionTemplate) {
        return new TaskStateService(taskRepository, taskDependencyService, concurrencyGuard, queueRedisService, transactionTemplate);
    }

    @Bean
    @ConditionalOnMissingBean
    public GroupFallbackService groupFallbackService(SchedulerProperties properties,
                                                     TaskRepository taskRepository,
                                                     GroupConfigRepository groupConfigRepository,
                                                     TaskDependencyService taskDependencyService,
                                                     QueueRedisService queueRedisService,
                                                     TransactionTemplate transactionTemplate) {
        return new GroupFallbackService(properties, taskRepository, groupConfigRepository,
                taskDependencyService, queueRedisService, transactionTemplate);
    }

    @Bean
    @ConditionalOnMissingBean
    public GroupFallbackScanner groupFallbackScanner(
            SchedulerProperties properties,
            TaskRepository taskRepository,
            TaskHandlerRegistry taskHandlerRegistry,
            TaskStateService taskStateService,
            GroupFallbackService groupFallbackService,
            @Qualifier("fallbackCallbackExecutor") ThreadPoolExecutor fallbackCallbackExecutor) {
        return new GroupFallbackScanner(properties, taskRepository, taskHandlerRegistry,
                taskStateService, groupFallbackService, fallbackCallbackExecutor);
    }

    @Bean
    @ConditionalOnMissingBean
    public WorkerService workerService(SchedulerProperties properties,
                                       TaskRepository taskRepository,
                                       TaskHandlerRegistry handlerRegistry,
                                       BusinessTaskStateProviderRegistry businessTaskStateProviderRegistry,
                                       ConcurrencyGuard concurrencyGuard,
                                       QueueRedisService queueRedisService,
                                       RecoveryService recoveryService,
                                       TaskStateService taskStateService,
                                       GroupConfigRepository groupConfigRepository,
                                       TransactionTemplate transactionTemplate,
                                       @Qualifier("schedulerWorkerExecutor") ThreadPoolTaskExecutor schedulerWorkerExecutor) {
        ensureInstanceId(properties);
        return new WorkerService(properties, taskRepository, handlerRegistry, concurrencyGuard,
                queueRedisService, recoveryService, schedulerWorkerExecutor, businessTaskStateProviderRegistry,
                taskStateService, groupConfigRepository, transactionTemplate);
    }

    @Bean
    @ConditionalOnMissingBean
    public DispatchService dispatchService(SchedulerProperties properties,
                                           GroupConfigRepository groupConfigRepository,
                                           TaskRepository taskRepository,
                                           QueueRedisService queueRedisService,
                                           ConcurrencyGuard concurrencyGuard,
                                           DynamicUserLimitService dynamicUserLimitService,
                                           WorkerService workerService,
                                           RecoveryService recoveryService,
                                           TaskHandlerRegistry taskHandlerRegistry,
                                           BusinessTaskStateProviderRegistry businessTaskStateProviderRegistry,
                                           TaskStateService taskStateService) {
        ensureInstanceId(properties);
        return new DispatchService(properties, groupConfigRepository, taskRepository, queueRedisService,
                concurrencyGuard, dynamicUserLimitService, workerService, recoveryService, taskHandlerRegistry,
                businessTaskStateProviderRegistry, taskStateService);
    }

    @Bean
    @ConditionalOnMissingBean
    public RecoveryService recoveryService(SchedulerProperties properties,
                                           TaskRepository taskRepository,
                                           ConcurrencyGuard concurrencyGuard,
                                           QueueRedisService queueRedisService,
                                           TaskStateService taskStateService) {
        return new RecoveryService(properties, taskRepository, concurrencyGuard, queueRedisService, taskStateService);
    }

    @Bean
    @ConditionalOnMissingBean
    public SchedulerJobs schedulerJobs(DispatchService dispatchService,
                                       RecoveryService recoveryService,
                                       GroupConfigRepository groupConfigRepository,
                                       GroupFallbackScanner groupFallbackScanner,
                                       SchedulerProperties properties) {
        return new SchedulerJobs(dispatchService, recoveryService, groupConfigRepository,
                groupFallbackScanner, properties);
    }

    @Bean
    @ConditionalOnMissingBean
    public SchedulerJobRunner schedulerJobRunner(SchedulerProperties properties, SchedulerJobs jobs) {
        return new SchedulerJobRunner(properties, jobs);
    }

    @Bean
    @ConditionalOnMissingBean
    public SchedulerClient schedulerClient(TaskRepository taskRepository,
                                           QueueRedisService queueRedisService,
                                           SchedulerProperties properties,
                                           TaskStateService taskStateService,
                                           GroupConfigRepository groupConfigRepository,
                                           DynamicUserLimitService dynamicUserLimitService,
                                           ConcurrencyGuard concurrencyGuard,
                                           WorkerService workerService) {
        ensureInstanceId(properties);
        return new DefaultSchedulerClient(
                taskRepository,
                queueRedisService,
                properties,
                taskStateService,
                groupConfigRepository,
                dynamicUserLimitService,
                concurrencyGuard,
                workerService
        );
    }

    @Bean
    @ConditionalOnMissingBean
    public DefaultGroupInitializer defaultGroupInitializer(SchedulerProperties properties,
                                                           org.springframework.jdbc.core.JdbcTemplate jdbcTemplate) {
        return new DefaultGroupInitializer(properties, jdbcTemplate);
    }

    private static String defaultInstanceId() {
        try {
            return InetAddress.getLocalHost().getHostName() + "-" + UUID.randomUUID();
        } catch (Exception e) {
            return "instance-" + UUID.randomUUID();
        }
    }

    private static void ensureInstanceId(SchedulerProperties properties) {
        if (properties.getInstanceId() == null || properties.getInstanceId().isBlank()) {
            properties.setInstanceId(defaultInstanceId());
        }
    }

    private static void validateFallbackProperties(SchedulerProperties properties) {
        if (properties.getFallbackCallbackThreads() < 1) {
            throw new IllegalArgumentException("fallbackCallbackThreads must be at least 1");
        }
        if (properties.getFallbackCallbackQueueCapacity() != 0) {
            throw new IllegalArgumentException("fallbackCallbackQueueCapacity must be 0");
        }
        if (properties.getFallbackPolicyTimeoutMs() < 1) {
            throw new IllegalArgumentException("fallbackPolicyTimeoutMs must be at least 1");
        }
        if (properties.getFallbackExecutorRejectBackoffMs() < properties.getFallbackMinNextCheckDelayMs()) {
            throw new IllegalArgumentException(
                    "fallbackExecutorRejectBackoffMs must be no less than fallbackMinNextCheckDelayMs");
        }
    }
}
