package org.dong.scheduler.core.util;

import org.slf4j.MDC;

import java.util.UUID;
import java.util.concurrent.Callable;

/**
 * 线程上下文工具类，用于在异步/定时任务中透传 traceId。
 */
public final class ThreadContextUtil {
    private static final String TRACE_ID = "trace_id";

    private ThreadContextUtil() {
    }

    /**
     * 执行 runnable，并设置指定的 traceId。
     */
    public static void runFunc(Runnable runnable, String traceId) {
        String previous = MDC.get(TRACE_ID);
        try {
            MDC.put(TRACE_ID, normalize(traceId));
            runnable.run();
        } finally {
            restore(previous);
        }
    }

    /**
     * 执行 runnable，复用当前 MDC 中的 traceId，不存在时生成新 UUID。
     */
    public static void runFunc(Runnable runnable) {
        runFunc(runnable, getOrCreate());
    }

    /**
     * 执行 callable，并设置指定的 traceId。
     */
    public static <T> T callFunc(Callable<T> callable, String traceId) throws Exception {
        String previous = MDC.get(TRACE_ID);
        try {
            MDC.put(TRACE_ID, normalize(traceId));
            return callable.call();
        } finally {
            restore(previous);
        }
    }

    /**
     * 包装 runnable，捕获当前 traceId 后在执行时注入（适合提交到线程池）。
     */
    public static Runnable addContext(Runnable runnable) {
        String traceId = getOrCreate();
        return () -> runFunc(runnable, traceId);
    }

    /**
     * 包装 callable，捕获当前 traceId 后在执行时注入（适合提交到线程池）。
     */
    public static <T> Callable<T> addContext(Callable<T> callable) {
        String traceId = getOrCreate();
        return () -> callFunc(callable, traceId);
    }

    /**
     * 包装 runnable，生成全新 traceId 后在执行时注入。
     */
    public static Runnable addNewContext(Runnable runnable) {
        String traceId = newTraceId();
        return () -> runFunc(runnable, traceId);
    }

    /**
     * 生成一个全新的 traceId。
     */
    public static String newTraceId() {
        return UUID.randomUUID().toString();
    }

    /**
     * 捕获当前 MDC 中的 traceId，不存在时生成新 UUID。
     */
    public static String captureTraceId() {
        return getOrCreate();
    }

    private static String getOrCreate() {
        String traceId = MDC.get(TRACE_ID);
        return (traceId == null || traceId.isBlank()) ? newTraceId() : traceId;
    }

    private static String normalize(String traceId) {
        return (traceId == null || traceId.isBlank()) ? newTraceId() : traceId;
    }

    private static void restore(String previous) {
        if (previous == null || previous.isBlank()) {
            MDC.remove(TRACE_ID);
            return;
        }
        MDC.put(TRACE_ID, previous);
    }
}
