package com.jarcadia.retask;

import java.lang.reflect.Method;

class WorkerHandlerMethod {
    
    enum HandlerType {
        Task, Change, Insert, Delete
    }

    private final HandlerType type;
    private final Class<?> clazz;
    private final Method method;

    public WorkerHandlerMethod(HandlerType type, Class<?> clazz, Method method) {
        this.type = type;
        this.clazz = clazz;
        this.method = method;
    }

    public HandlerType getType() {
        return type;
    }

    public Class<?> getWorkerClass() {
        return clazz;
    }
    
    public Method getMethod() {
        return method;
    }
}
