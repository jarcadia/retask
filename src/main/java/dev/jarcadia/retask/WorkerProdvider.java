package dev.jarcadia.retask;

@FunctionalInterface
public interface WorkerProdvider {
    public Object getInstance(Class<?> clazz);
}
