package icu.wwj.elasticjob.external.reg.kubernetes;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Getter
public final class KubernetesRegistryConfiguration {
    
    private final String namespace;
    
    private final int ephemeralEachRenewalSeconds; 
    
    private final int ephemeralRenewIntervalSeconds;
    
    public KubernetesRegistryConfiguration(final String namespace) {
        this(namespace, 30, 9);
    }
}
