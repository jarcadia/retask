package dev.jarcadia.iface;

import dev.jarcadia.Fields;

@FunctionalInterface
public interface DmlEventHandler {
    Object apply(String table, Fields fields) throws Throwable;
}
