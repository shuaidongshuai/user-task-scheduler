package org.dong.scheduler.core.service;

import org.dong.scheduler.core.enums.BusinessTaskState;
import org.dong.scheduler.core.model.SchedulerTask;
import org.dong.scheduler.core.model.TaskExecuteResult;
import org.dong.scheduler.core.spi.BusinessTaskStateProvider;
import org.dong.scheduler.core.spi.TaskHandler;
import org.junit.jupiter.api.Test;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.ObjectProvider;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

class RegistryLazyInitTest {

    @Test
    void shouldNotInitializeTaskHandlersUntilFirstLookup() {
        AtomicInteger streamCalls = new AtomicInteger();
        TaskHandler handler = new TaskHandler() {
            @Override
            public List<String> bizTypes() {
                return List.of("demo.biz");
            }

            @Override
            public TaskExecuteResult execute(SchedulerTask task) {
                return TaskExecuteResult.success();
            }
        };

        TaskHandlerRegistry registry = new TaskHandlerRegistry(new CountingObjectProvider<>(List.of(handler), streamCalls));

        assertEquals(0, streamCalls.get());
        assertSame(handler, registry.find("demo.biz"));
        assertEquals(1, streamCalls.get());
        assertSame(handler, registry.find("demo.biz"));
        assertEquals(1, streamCalls.get());
        assertNull(registry.find("missing.biz"));
        assertEquals(1, streamCalls.get());
    }

    @Test
    void shouldNotInitializeStateProvidersUntilFirstLookup() {
        AtomicInteger streamCalls = new AtomicInteger();
        BusinessTaskStateProvider provider = new BusinessTaskStateProvider() {
            @Override
            public String bizType() {
                return "demo.biz";
            }

            @Override
            public BusinessTaskState query(SchedulerTask task) {
                return BusinessTaskState.NEED_RUNNING;
            }
        };

        BusinessTaskStateProviderRegistry registry = new BusinessTaskStateProviderRegistry(
                new CountingObjectProvider<>(List.of(provider), streamCalls)
        );

        assertEquals(0, streamCalls.get());
        assertSame(provider, registry.find("demo.biz"));
        assertEquals(1, streamCalls.get());
        assertSame(provider, registry.find("demo.biz"));
        assertEquals(1, streamCalls.get());
        assertNull(registry.find("missing.biz"));
        assertEquals(1, streamCalls.get());
    }

    private static final class CountingObjectProvider<T> implements ObjectProvider<T> {
        private final List<T> values;
        private final AtomicInteger streamCalls;

        private CountingObjectProvider(List<T> values, AtomicInteger streamCalls) {
            this.values = values;
            this.streamCalls = streamCalls;
        }

        @Override
        public T getObject() throws BeansException {
            if (values.isEmpty()) {
                throw new IllegalStateException("no bean available");
            }
            return values.getFirst();
        }

        @Override
        public T getObject(Object... args) throws BeansException {
            if (values.isEmpty()) {
                throw new IllegalStateException("no bean available");
            }
            return values.getFirst();
        }

        @Override
        public T getIfAvailable() throws BeansException {
            return values.isEmpty() ? null : values.getFirst();
        }

        @Override
        public T getIfUnique() throws BeansException {
            return values.size() == 1 ? values.getFirst() : null;
        }

        @Override
        public Iterator<T> iterator() {
            return values.iterator();
        }

        @Override
        public Stream<T> orderedStream() {
            streamCalls.incrementAndGet();
            return values.stream();
        }
    }
}
