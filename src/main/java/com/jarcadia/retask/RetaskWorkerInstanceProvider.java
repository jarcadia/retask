package com.jarcadia.retask;

@FunctionalInterface
public interface RetaskWorkerInstanceProvider {
    public Object getInstance(Class<?> clazz);
}
