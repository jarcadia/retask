package dev.jarcadia.iface;

import dev.jarcadia.Fields;

@FunctionalInterface
public interface ReturnValueHandler<T> {
    void handle(T returned) throws Exception;
}
