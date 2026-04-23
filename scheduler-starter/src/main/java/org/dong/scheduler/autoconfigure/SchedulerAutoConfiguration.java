package org.dong.scheduler.autoconfigure;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.dong.scheduler.config.SchedulerProperties;
import org.dong.scheduler.core.job.SchedulerJobs;
import org.dong.scheduler.core.redis.ConcurrencyGuard;
import org.dong.scheduler.core.redis.QueueRedisService;
import org.dong.scheduler.core.redis.RedisConcurrencyGuard;
import org.dong.scheduler.core.repo.GroupConfigRepository;
import org.dong.scheduler.core.repo.JdbcGroupConfigRepository;
import org.dong.scheduler.core.repo.JdbcTaskRepository;
import org.dong.scheduler.core.repo.TaskRepository;
import org.dong.scheduler.core.service.DefaultSchedulerClient;
import org.dong.scheduler.core.service.DispatchService;
import org.dong.scheduler.core.service.DynamicUserLimitService;
import org.dong.scheduler.core.service.RecoveryService;
import org.dong.scheduler.core.service.TaskHandlerRegistry;
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

import java.net.InetAddress;
import java.util.Optional;
import java.util.UUID;

@AutoConfiguration
@EnableScheduling
@EnableConfigurationProperties(SchedulerProperties.class)
@ConditionalOnProperty(prefix = "scheduler", name = "enabled", havingValue = "true", matchIfMissing = true)
@ConditionalOnClass({org.springframework.jdbc.core.JdbcTemplate.class, org.springframework.data.redis.core.StringRedisTemplate.class})
public class SchedulerAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public TaskRepository taskRepository(org.springframework.jdbc.core.JdbcTemplate jdbcTemplate) {
        return new JdbcTaskRepository(jdbcTemplate);
    }

    @Bean
    @ConditionalOnMissingBean
    public GroupConfigRepository groupConfigRepository(org.springframework.jdbc.core.JdbcTemplate jdbcTemplate) {
        return new JdbcGroupConfigRepository(jdbcTemplate);
    }

    @Bean
    @ConditionalOnMissingBean
    public QueueRedisService queueRedisService(org.springframework.data.redis.core.StringRedisTemplate redisTemplate) {
        return new QueueRedisService(redisTemplate);
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
    public TaskHandlerRegistry taskHandlerRegistry(ObjectProvider<TaskHandler> handlers) {
        return new TaskHandlerRegistry(handlers.orderedStream().toList());
    }

    @Bean
    @ConditionalOnMissingBean(name = "schedulerWorkerExecutor")
    public ThreadPoolTaskExecutor schedulerWorkerExecutor(SchedulerProperties properties) {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setThreadNamePrefix("sched-worker-");
        executor.setCorePoolSize(properties.getWorkerThreads());
        executor.setMaxPoolSize(properties.getWorkerThreads());
        executor.setQueueCapacity(properties.getWorkerThreads() * 10);
        executor.initialize();
        return executor;
    }

    @Bean
    @ConditionalOnMissingBean
    public WorkerService workerService(SchedulerProperties properties,
                                       TaskRepository taskRepository,
                                       TaskHandlerRegistry handlerRegistry,
                                       ConcurrencyGuard concurrencyGuard,
                                       QueueRedisService queueRedisService,
                                       @Qualifier("schedulerWorkerExecutor") ThreadPoolTaskExecutor schedulerWorkerExecutor,
                                       Optional<BusinessTaskStateProvider> businessTaskStateProvider) {
        ensureInstanceId(properties);
        return new WorkerService(properties, taskRepository, handlerRegistry, concurrencyGuard,
                queueRedisService, schedulerWorkerExecutor, businessTaskStateProvider);
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
                                           Optional<BusinessTaskStateProvider> businessTaskStateProvider) {
        ensureInstanceId(properties);
        return new DispatchService(properties, groupConfigRepository, taskRepository, queueRedisService,
                concurrencyGuard, dynamicUserLimitService, workerService, businessTaskStateProvider);
    }

    @Bean
    @ConditionalOnMissingBean
    public RecoveryService recoveryService(SchedulerProperties properties,
                                           TaskRepository taskRepository,
                                           ConcurrencyGuard concurrencyGuard,
                                           QueueRedisService queueRedisService) {
        return new RecoveryService(properties, taskRepository, concurrencyGuard, queueRedisService);
    }

    @Bean
    @ConditionalOnMissingBean
    public SchedulerJobs schedulerJobs(DispatchService dispatchService,
                                       RecoveryService recoveryService,
                                       GroupConfigRepository groupConfigRepository) {
        return new SchedulerJobs(dispatchService, recoveryService, groupConfigRepository);
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
                                           ObjectMapper objectMapper) {
        return new DefaultSchedulerClient(taskRepository, queueRedisService, objectMapper);
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
}
