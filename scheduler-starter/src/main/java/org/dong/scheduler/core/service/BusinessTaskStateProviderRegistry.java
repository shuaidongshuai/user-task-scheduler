package org.dong.scheduler.core.service;

import org.dong.scheduler.core.spi.BusinessTaskStateProvider;
import org.springframework.beans.factory.ObjectProvider;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BusinessTaskStateProviderRegistry {
    private final ObjectProvider<BusinessTaskStateProvider> providerSource;
    private volatile Map<String, BusinessTaskStateProvider> providers;

    public BusinessTaskStateProviderRegistry(List<BusinessTaskStateProvider> providers) {
        this.providerSource = null;
        this.providers = buildIndex(providers);
    }

    public BusinessTaskStateProviderRegistry(ObjectProvider<BusinessTaskStateProvider> providerSource) {
        this.providerSource = providerSource;
        this.providers = null;
    }

    public BusinessTaskStateProvider find(String bizType) {
        return providers().get(bizType);
    }

    private Map<String, BusinessTaskStateProvider> providers() {
        Map<String, BusinessTaskStateProvider> current = providers;
        if (current != null) {
            return current;
        }
        synchronized (this) {
            current = providers;
            if (current == null) {
                if (providerSource == null) {
                    current = Map.of();
                } else {
                    current = buildIndex(providerSource.orderedStream().toList());
                }
                providers = current;
            }
        }
        return current;
    }

    private Map<String, BusinessTaskStateProvider> buildIndex(List<BusinessTaskStateProvider> providers) {
        Map<String, BusinessTaskStateProvider> index = new HashMap<>();
        for (BusinessTaskStateProvider provider : providers) {
            String bizType = provider.bizType();
            BusinessTaskStateProvider existed = index.putIfAbsent(bizType, provider);
            if (existed != null) {
                throw new IllegalStateException("duplicate BusinessTaskStateProvider bizType=" + bizType
                        + ", existed=" + existed.getClass().getName()
                        + ", incoming=" + provider.getClass().getName());
            }
        }
        return Map.copyOf(index);
    }
}
