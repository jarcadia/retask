package dev.jarcadia;

@FunctionalInterface
public interface InstanceProvider {
    public Object getInstance(Class<?> clazz);
}
