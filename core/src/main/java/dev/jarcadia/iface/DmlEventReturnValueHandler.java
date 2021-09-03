package dev.jarcadia.iface;

import dev.jarcadia.Fields;

@FunctionalInterface
public interface DmlEventReturnValueHandler {
    boolean handle(String table, Fields fields, Object returned);
}
