package com.jarcadia.retask;

@FunctionalInterface
public interface RetaskContext {
    public Object getInstance(Class<?> clazz);
}
