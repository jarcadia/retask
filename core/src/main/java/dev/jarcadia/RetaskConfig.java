package dev.jarcadia;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import dev.jarcadia.iface.DmlEventReturnValueHandler;
import dev.jarcadia.iface.SerializationCustomizer;
import dev.jarcadia.iface.TaskReturnValueHandler;
import io.lettuce.core.RedisClient;

public class RetaskConfig {

    private final ObjectMapper objectMapper;
    private final SimpleModule module;

    private RedisClient redisClient;
    private TaskReturnValueHandler taskReturnValueHandler;
    private DmlEventReturnValueHandler dmlEventReturnValueHandler;
    private boolean flushDatabase;

    protected RetaskConfig() {
        this.objectMapper = new ObjectMapper();
        this.module = new SimpleModule();
    }

    public RetaskConfig usingRedis(RedisClient redisClient) {
        this.redisClient = redisClient;
        return this;
    }

    public RetaskConfig customizeSerialization(SerializationCustomizer customizer) {
        customizer.customize(module, objectMapper, objectMapper.getTypeFactory());
        this.objectMapper.registerModule(module);
        return this;
    }

    public RetaskConfig withTaskReturnValueHandler(TaskReturnValueHandler taskReturnValueHandler) {
        this.taskReturnValueHandler = taskReturnValueHandler;
        return this;
    }

    public RetaskConfig withDmlEventReturnValueHandler(DmlEventReturnValueHandler dmlEventReturnValueHandler) {
        this.dmlEventReturnValueHandler = dmlEventReturnValueHandler;
        return this;
    }

    public RetaskConfig flushDatabase() {
        this.flushDatabase = true;
        return this;
    }

    public Retask create() {
        return new Retask(redisClient, objectMapper, taskReturnValueHandler, dmlEventReturnValueHandler, flushDatabase);
    }
}
