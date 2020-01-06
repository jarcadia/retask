package com.jarcadia.retask;

import java.lang.reflect.Method;

class WorkerHandlerMethod {

    private final Class<?> clazz;
    private final Method method;

    public WorkerHandlerMethod(Class<?> clazz, Method method) {
        this.clazz = clazz;
        this.method = method;
    }

    public Class<?> getWorkerClass() {
        return clazz;
    }
    
    public Method getMethod() {
        return method;
    }
}
