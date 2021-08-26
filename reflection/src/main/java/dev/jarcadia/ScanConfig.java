package dev.jarcadia;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import io.lettuce.core.RedisClient;

import java.util.HashSet;
import java.util.Set;

public class ScanConfig {


    private final Set<String> scanPackages;
    private final Set<InstanceProvider> instanceProviders;

    protected ScanConfig() {
        this.scanPackages = new HashSet<>();
        this.instanceProviders = new HashSet<>();
    }

    public ScanConfig scanPackageForAnnotatedHandlers(String packageName) {
        this.scanPackages.add(packageName);
        return this;
    }

    public ScanConfig addInstanceProvider(InstanceProvider instanceProvider) {
        this.instanceProviders.add(instanceProvider);
        return this;
    }

    protected Set<String> getScanPackages() {
        return scanPackages;
    }

    protected Set<InstanceProvider> getInstanceProviders() {
        return instanceProviders;
    }

}
