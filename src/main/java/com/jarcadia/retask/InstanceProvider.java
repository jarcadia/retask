package com.jarcadia.retask;

@FunctionalInterface
public interface InstanceProvider {
    public Object getInstance(Class<?> clazz);
}
