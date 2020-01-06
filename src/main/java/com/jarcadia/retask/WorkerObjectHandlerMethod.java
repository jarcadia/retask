package com.jarcadia.retask;

import java.lang.reflect.Method;

public class WorkerObjectHandlerMethod extends WorkerHandlerMethod {
    
    private final String mapKey;

    public WorkerObjectHandlerMethod(Class<?> clazz, Method method, String mapKey) {
        super(clazz, method);
        this.mapKey = mapKey;
    }
    
    public String getMapKey() {
        return this.mapKey;
    }
}
