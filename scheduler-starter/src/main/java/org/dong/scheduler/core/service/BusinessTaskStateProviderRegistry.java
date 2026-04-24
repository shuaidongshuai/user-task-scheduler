package org.dong.scheduler.core.service;

import org.dong.scheduler.core.spi.BusinessTaskStateProvider;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BusinessTaskStateProviderRegistry {
    private final Map<String, BusinessTaskStateProvider> providers;

    public BusinessTaskStateProviderRegistry(List<BusinessTaskStateProvider> providers) {
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
        this.providers = Map.copyOf(index);
    }

    public BusinessTaskStateProvider find(String bizType) {
        return providers.get(bizType);
    }
}
