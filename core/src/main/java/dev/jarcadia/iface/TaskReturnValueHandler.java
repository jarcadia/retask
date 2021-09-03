package dev.jarcadia.iface;

import dev.jarcadia.Fields;

@FunctionalInterface
public interface TaskReturnValueHandler {
    boolean handle(String taskId, String route, int attempt, int permit, Fields fields, Object returned);
}
