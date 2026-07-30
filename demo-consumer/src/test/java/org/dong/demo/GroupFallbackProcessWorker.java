package org.dong.demo;

import org.dong.scheduler.config.SchedulerProperties;
import org.dong.scheduler.core.enums.TaskStatus;
import org.dong.scheduler.core.model.GroupConfig;
import org.dong.scheduler.core.model.GroupFallbackDecision;
import org.dong.scheduler.core.model.SchedulerTask;
import org.dong.scheduler.core.model.TaskExecuteResult;
import org.dong.scheduler.core.model.TaskDependencyRequest;
import org.dong.scheduler.core.redis.QueueRedisService;
import org.dong.scheduler.core.repo.GroupConfigRepository;
import org.dong.scheduler.core.repo.JdbcTaskRepository;
import org.dong.scheduler.core.service.GroupFallbackScanner;
import org.dong.scheduler.core.service.GroupFallbackService;
import org.dong.scheduler.core.service.TaskDependencyService;
import org.dong.scheduler.core.service.TaskHandlerRegistry;
import org.dong.scheduler.core.spi.TaskHandler;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.support.TransactionTemplate;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/** Separate-process worker used to prove fallback CAS behavior across independent JVMs. */
public final class GroupFallbackProcessWorker {
    private static final String ROUTE = "codex-fallback-it";
    private static final String TARGET_GROUP = "codex_fallback_target";
    private static final String BIZ_TYPE = "codex.fallback.process";

    private GroupFallbackProcessWorker() {
    }

    public static void main(String[] args) throws Exception {
        long taskId = Long.parseLong(args[0]);
        Path barrierDirectory = Path.of(args[1]);
        String instance = args[2];
        DriverManagerDataSource dataSource = dataSource();
        JdbcTaskRepository taskRepository = new JdbcTaskRepository(new JdbcTemplate(dataSource));
        SchedulerProperties properties = properties();
        LettuceConnectionFactory redisConnectionFactory = redisConnectionFactory();
        ThreadPoolExecutor executor = executor(instance);
        try {
            QueueRedisService queueRedisService = new QueueRedisService(
                    new StringRedisTemplate(redisConnectionFactory), properties);
            GroupFallbackService fallbackService = new GroupFallbackService(
                    properties, taskRepository, groupRepository(), dependencyService(), queueRedisService,
                    new TransactionTemplate(new DataSourceTransactionManager(dataSource)));
            TaskHandler handler = handler(taskId, barrierDirectory, instance);
            GroupFallbackScanner scanner = new GroupFallbackScanner(
                    properties, taskRepository, new TaskHandlerRegistry(List.of(handler)),
                    null, fallbackService, executor);
            int changed = scanner.scanOnce();
            System.out.println("FALLBACK_CHANGED=" + changed);
        } finally {
            executor.shutdownNow();
            redisConnectionFactory.destroy();
        }
    }

    private static DriverManagerDataSource dataSource() {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName("com.mysql.cj.jdbc.Driver");
        dataSource.setUrl(requiredEnvironment("DEMO_DB_URL"));
        dataSource.setUsername(requiredEnvironment("DEMO_DB_USERNAME"));
        dataSource.setPassword(requiredEnvironment("DEMO_DB_PASSWORD"));
        return dataSource;
    }

    private static SchedulerProperties properties() {
        SchedulerProperties properties = new SchedulerProperties();
        properties.setDispatchRoute(ROUTE);
        properties.setFallbackScanLimit(10);
        properties.setFallbackCallbackThreads(1);
        properties.setFallbackPolicyTimeoutMs(10_000L);
        return properties;
    }

    private static LettuceConnectionFactory redisConnectionFactory() {
        String host = System.getenv().getOrDefault("DEMO_REDIS_HOST", "127.0.0.1");
        int port = Integer.parseInt(System.getenv().getOrDefault("DEMO_REDIS_PORT", "6379"));
        LettuceConnectionFactory factory = new LettuceConnectionFactory(
                new RedisStandaloneConfiguration(host, port));
        factory.afterPropertiesSet();
        factory.start();
        return factory;
    }

    private static ThreadPoolExecutor executor(String instance) {
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                1, 1, 0L, TimeUnit.MILLISECONDS, new SynchronousQueue<>(), runnable -> {
                    Thread thread = new Thread(runnable, "fallback-process-" + instance);
                    thread.setDaemon(true);
                    return thread;
                });
        return executor;
    }

    private static GroupConfigRepository groupRepository() {
        return new GroupConfigRepository() {
            @Override
            public List<GroupConfig> listEnabled() {
                return List.of(group());
            }

            @Override
            public Optional<GroupConfig> findEnabledByGroupCode(String groupCode) {
                return TARGET_GROUP.equals(groupCode) ? Optional.of(group()) : Optional.empty();
            }

            private GroupConfig group() {
                GroupConfig group = new GroupConfig();
                group.setGroupCode(TARGET_GROUP);
                group.setEnabled(true);
                return group;
            }
        };
    }

    private static TaskDependencyService dependencyService() {
        return new TaskDependencyService() {
            @Override
            public void createDependencies(Long taskId, List<TaskDependencyRequest> dependencies,
                                           LocalDateTime now) {
            }

            @Override
            public SchedulerTask refreshTaskAfterSubmit(Long taskId, LocalDateTime now) {
                return null;
            }

            @Override
            public List<SchedulerTask> onUpstreamTaskTerminal(Long upstreamTaskId, TaskStatus actualStatus,
                                                               LocalDateTime now) {
                return List.of();
            }
        };
    }

    private static TaskHandler handler(long expectedTaskId, Path barrierDirectory, String instance) {
        return new TaskHandler() {
            @Override
            public List<String> bizTypes() {
                return List.of(BIZ_TYPE);
            }

            @Override
            public TaskExecuteResult execute(SchedulerTask task) {
                return TaskExecuteResult.success();
            }

            @Override
            public GroupFallbackDecision onGroupWaitTimeout(SchedulerTask task) {
                if (task.getId() != expectedTaskId) {
                    return GroupFallbackDecision.stopChecking();
                }
                waitAtBarrier(barrierDirectory, instance);
                return GroupFallbackDecision.routeTo(TARGET_GROUP, null);
            }
        };
    }

    private static void waitAtBarrier(Path barrierDirectory, String instance) {
        try {
            Files.createFile(barrierDirectory.resolve("ready-" + instance));
            long deadline = System.currentTimeMillis() + 15_000L;
            while (!Files.exists(barrierDirectory.resolve("go"))) {
                if (System.currentTimeMillis() >= deadline) {
                    throw new IllegalStateException("cross-JVM fallback barrier timed out");
                }
                Thread.sleep(20L);
            }
        } catch (Exception ex) {
            throw new IllegalStateException("cross-JVM fallback barrier failed", ex);
        }
    }

    private static String requiredEnvironment(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("missing environment variable " + name);
        }
        return value;
    }
}
